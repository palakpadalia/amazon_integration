import requests
import urllib.parse
import frappe
import datetime
from erpnext.controllers.accounts_controller import get_taxes_and_charges

# ? GET TOKEN FROM AMAZON SETTINGS
def get_access_token(refresh_token, lwa_app_id, lwa_client_secret):
    """
    Get Amazon API access token.
    
    Args:
        refresh_token (str): OAuth refresh token
        lwa_app_id (str): Amazon LWA app ID
        lwa_client_secret (str): Amazon LWA client secret
    
    Returns:
        str: Access token
    
    Raises:
        RequestException: If token fetch fails
    """
    try:
        # ? USES OAUTH 2.0 TOKEN ENDPOINT
        token_response = requests.post(
            "https://api.amazon.com/auth/o2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": lwa_app_id,
                "client_secret": lwa_client_secret,
            },
        )
        token_response.raise_for_status()
        return token_response.json().get("access_token")

    except requests.exceptions.RequestException as e:
        # ! CRITICAL ERROR - WITHOUT TOKEN, NO API ACCESS POSSIBLE
        frappe.log_error(str(e), "Access Token Error")
        raise

# ? GET ORDERS FROM AMAZON API WITH TOKENS
def get_orders(endpoint, request_params, access_token):
    """
    Fetch orders from Amazon Vendor API.
    
    Args:
        endpoint (str): API endpoint URL
        request_params (dict): Query parameters
        access_token (str): Valid access token
    
    Returns:
        dict: JSON response with orders data
    """
    try:
        # * CONSTRUCT URL WITH PROPER ENCODING
        response = requests.get(
            f"{endpoint}/vendor/orders/v1/purchaseOrders?"
            + urllib.parse.urlencode(request_params),
            headers={"x-amz-access-token": access_token},
        )
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        # ? RETURN EMPTY ORDERS LIST AS FALLBACK
        frappe.log_error(str(e), "Fetch Orders Error")
        return {"payload": {"orders": []}}

# 
@frappe.whitelist()
def sync_amazon_vendor_orders(created_after=None, created_before=None):
    """
    Main synchronization function for Amazon vendor orders.
    
    Args:
        created_after (str, optional): Start date for order sync
        created_before (str, optional): End date for order sync
    
    Returns:
        list: Processed orders
    """
    # * GET API CREDENTIALS AND SETTINGS
    credentials = get_credentials(
        "Amazon Settings",
        fields=[
            "refresh_token",
            "lwa_app_id",
            "lwa_client_secret",
            "endpoint",
            "marketplace_id",
            "amazon_sales_person",
            "enable",
        ],
    )

    # ? EARLY RETURN IF INTEGRATION IS DISABLED
    enabled = credentials["enable"]
    if not enabled:
        return

    # * EXTRACT CREDENTIALS
    refresh_token = credentials["refresh_token"]
    lwa_app_id = credentials["lwa_app_id"]
    lwa_client_secret = credentials["lwa_client_secret"]
    marketplace_id = credentials["marketplace_id"]
    endpoint = credentials["endpoint"]
    sales_person = credentials["amazon_sales_person"]

    # ! CRITICAL: GET FRESH ACCESS TOKEN FOR API ACCESS
    access_token = get_access_token(refresh_token, lwa_app_id, lwa_client_secret)

    # ? DEFAULT TO LAST 2 HOURS IF NO START DATE PROVIDED
    if not created_after:
        created_after = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=2)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

    # * PREPARE REQUEST PARAMETERS
    request_params = {
        "MarketplaceIds": marketplace_id,
        "createdAfter": created_after,
        "purchaseOrderState": "Acknowledged",
    }

    if created_before:
        request_params["createdBefore"] = created_before

    # * FETCH AND PROCESS ORDERS
    orders = get_orders(endpoint, request_params, access_token)
    orders_list = orders.get("payload", {}).get("orders", [])
    add_orders(orders_list, sales_person)

    return orders_list


@frappe.whitelist()
def add_orders(orders, sales_person):
    """Process multiple orders and create sales orders for new ones."""
    # * LOOP THROUGH ORDERS AND CREATE IF NOT EXISTING
    for order in orders:
        if order_does_not_exists(order):
            create_sales_order(order, sales_person)


@frappe.whitelist()
def order_does_not_exists(order):
    """Check if order already exists in ERPNext."""
    # ? PREVENT DUPLICATE ORDERS
    existing_order = frappe.db.exists(
        "Sales Order", {"custom_amazon_order_id": order["purchaseOrderNumber"]}
    )
    return not existing_order


@frappe.whitelist()
def get_customer_from_address(address_code):
    """
    Get customer details from address code.
    
    Args:
        address_code (str): Address identifier
    
    Returns:
        dict: Contains company name and address
    
    Raises:
        frappe.ValidationError: If customer does not exist
    """
    address = frappe.db.get_value(
        "Address", filters={"address_title": address_code}, fieldname=["name"]
    )

    company = frappe.db.get_value(
        "Dynamic Link", filters={"parent": address}, fieldname=["link_title"]
    )

    if not company:
        # Log missing customer information
        frappe.log_error(
            message=f"No customer found for party ID: {address_code}",
            title="Missing Customer for Amazon Order"
        )

    return {'address': address, 'company': company}


def get_default_warehouse():
    """
    Get default warehouse from Stock Settings.
    
    Returns:
        str: Default warehouse name
    """
    default_warehouse = frappe.db.get_single_value('Stock Settings', 'default_warehouse')
    if not default_warehouse:
        frappe.throw("Default warehouse not set in Stock Settings")
    return default_warehouse


@frappe.whitelist()
def create_sales_order(order, sales_person):
    """
    Create new sales order from Amazon order.

    Args:
        order (dict): Amazon order data
        sales_person (str): Sales person ID

    Returns:
        str: Created sales order name

    Raises:
        ValidationError: If order creation fails
    """
    try:
        sales_order = frappe.new_doc("Sales Order")
        missing_vendor_items = []

        # * EXTRACT AND VALIDATE DELIVERY DATE
        date_range = order.get("orderDetails", {}).get("deliveryWindow", "")
        delivery_date = None
        if date_range and "--" in date_range:
            try:
                delivery_date = date_range.split("--")[1].split("T")[0]
            except (IndexError, AttributeError):
                delivery_date = order.get("orderDetails", {}).get("purchaseOrderDate", "").split("T")[0]

        # ? FALLBACK DATES IF NOT FOUND
        if not delivery_date:
            delivery_date = order.get("orderDetails", {}).get("purchaseOrderDate", "").split("T")[0]

        if not delivery_date:
            delivery_date = frappe.utils.today()

        # * SET ORDER HEADER DETAILS
        sales_order.transaction_date = (
            order.get("orderDetails", {}).get("purchaseOrderDate", "").split("T")[0]
            or frappe.utils.today()
        )

        address_code = order.get("orderDetails", {}).get("buyingParty", {}).get("partyId", "")
        customer_data = get_customer_from_address(address_code)
        sales_order.customer = customer_data.get('company')
        sales_order.customer_address = customer_data.get('address')
        sales_order.custom_amazon_order_id = order.get("purchaseOrderNumber", "")

        sales_order.custom_sales_person = sales_person
        sales_order.order_type = "Sales"
        company = get_default_company()
        sales_order.company = company.default_company
        sales_order.currency = company.default_currency

        sales_order.delivery_date = delivery_date

        # * GET DEFAULT WAREHOUSE
        default_warehouse = get_default_warehouse()

        # * Set up taxes
        set_tax_and_charges_table(sales_order=sales_order)

        # * PROCESS ORDER ITEMS
        items = order.get("orderDetails", {}).get("items", [])
        for item in items:
            amazon_product_id = item.get("amazonProductIdentifier")
            try:
                item_code, uom = get_item_code(amazon_product_id)  # ? UNPACK THE TUPLE

                if not item_code:  # Skip if item_code is not found
                    missing_vendor_items.append(amazon_product_id)
                    continue

                sales_order.append(
                    "items",
                    {
                        "item_code": item_code,
                        "delivery_date": delivery_date,
                        "qty": int(item.get("orderedQuantity", {}).get("amount", 0)),
                        "rate": float(item.get("netCost", {}).get("amount", 0)),
                        "uom": uom or "NOS",  # ? USE FETCHED UOM, DEFAULT TO "NOS" IF NONE
                        "warehouse": default_warehouse  # ? SET DEFAULT WAREHOUSE FROM STOCK SETTINGS
                    },
                )

            except frappe.ValidationError as e:
                # ! LOG ITEM PROCESSING ERRORS
                error_msg = f"Error processing order {order.get('purchaseOrderNumber', '')}: {str(e)}"
                frappe.log_error(message=error_msg, title="Amazon Order Item Error")

        # * SAVE AND COMMIT SALES ORDER
        sales_order.save()
        frappe.db.commit()

        # * LOG MISSING VENDOR IDS IN SALES ORDER ITEM TRACKING DOCTYPE
        if missing_vendor_items:
            tracking_doc = frappe.new_doc("Sales Order Item Tracking")
            tracking_doc.sales_order = sales_order.name
            tracking_doc.old_items = {'Missing Items': missing_vendor_items}  # JSON FIELD

            # Log before saving
            frappe.log_error(message=f"Tracking Doc Data: {frappe.as_json(tracking_doc)}", title="Sales Order Item Tracking Debug")

            tracking_doc.save(ignore_permissions=True)
            frappe.db.commit()

            # Show message only once per sync execution
            if not getattr(frappe.flags, "missing_items_msg_shown", False):
                frappe.msgprint(
                    msg="Some items were not found in the system. Please check the 'Sales Order Item Tracking' list.",
                    title="Missing Items Warning",
                    indicator="orange"
                )
                frappe.flags.missing_items_msg_shown = True  # Set flag to prevent duplicates



        return sales_order.name

    except Exception as e:
        # ! LOG ORDER CREATION ERRORS
        frappe.log_error(message=str(e), title="Create Sales Order Error")
        raise frappe.ValidationError("Failed to create sales order. Check logs for details.")

@frappe.whitelist()
def get_tax_and_charges_template():
    """Get default tax template."""
    # * GET DEFAULT TAX TEMPLATE
    template = frappe.db.get_value(
        "Sales Taxes and Charges Template",
        filters={"is_default": 1},
        fieldname=["name", "tax_category"],
        as_dict=1,
    )
    return template


def get_default_company():
    """Get default company settings."""
    company = frappe.get_doc("Global Defaults")
    return company


@frappe.whitelist()
def get_credentials(doctype, fields):
    """Get credentials from specified doctype."""
    doc = frappe.get_doc(doctype)
    credentials = {field: getattr(doc, field, None) for field in fields}
    return credentials


@frappe.whitelist()
def get_item_code(vendor_id):
    """
    Get ERPNext item code and UOM from Amazon vendor ID stored in the Stock UOM Conversion table.
    
    Args:
        vendor_id (str): Amazon vendor ID
    
    Returns:
        tuple: (item_code, uom) or (None, None) if not found
    """
    # ? FIND UOM CONVERSION ENTRY WITH GIVEN AMAZON VENDOR ID
    uom_entry = frappe.db.get_value(
        "UOM Conversion Detail",
        filters={"custom_amazon_vendor_id": vendor_id},
        fieldname=["parent", "uom"],
        as_dict=True
    )

    if not uom_entry:
        # ? RETURN EMPTY VALUES INSTEAD OF THROWING AN ERROR
        return None, None  

    item_code = uom_entry.get("parent")  # Parent is the Item code
    uom = uom_entry.get("uom") or "NOS"  # Default UOM to NOS if not found

    return item_code, uom




def set_tax_and_charges_table(sales_order):
    """Set up tax and charges in sales order."""
    # * GET AND APPLY TAX TEMPLATE
    tax_and_charges_template = get_tax_and_charges_template()
    master_name = tax_and_charges_template["name"]
    tax_category = tax_and_charges_template["tax_category"]

    sales_order.tax_category = tax_category
    sales_order.taxes_and_charges = master_name

    # * APPLY TAX ENTRIES
    tax_entries = get_taxes_and_charges(
        master_doctype="Sales Taxes and Charges Template", master_name=master_name
    )

    if tax_entries:
        for tax in tax_entries:
            sales_order.append(
                "taxes",
                {
                    "charge_type": tax.get("charge_type", "On Net Total"),
                    "account_head": tax.get("account_head"),
                    "description": tax.get("description", ""),
                    "rate": tax.get("rate", 0.0),
                    "cost_center": tax.get("cost_center", ""),
                    "included_in_print_rate": tax.get("included_in_print_rate", 0),
                    "included_in_paid_amount": tax.get("included_in_paid_amount", 0),
                },
            )


def autoname(doc, method):
    """Generate custom name for Amazon orders."""
    # ? SET CUSTOM NAMING FORMAT FOR AMAZON ORDERS
    if doc.get("custom_amazon_order_id"):
        doc.name = f"AMZ-{doc.custom_amazon_order_id}"


