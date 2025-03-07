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
    created_orders = []
    skipped_orders = []
    error_orders = []
    
    # * LOOP THROUGH ORDERS AND CREATE IF NOT EXISTING
    for order in orders:
        try:
            amazon_order_id = order.get("purchaseOrderNumber", "Unknown")
            
            # Check if order already exists
            if not order_does_not_exists(order):
                skipped_orders.append(amazon_order_id)
                continue
            
            # Create the sales order
            new_order = create_sales_order(order, sales_person)
            
            # Track successfully created orders
            if new_order:
                created_orders.append(new_order)
        except Exception as e:
            # * TRACK FAILED ORDERS BUT CONTINUE PROCESSING OTHERS
            amazon_order_id = order.get("purchaseOrderNumber", "Unknown")
            error_orders.append(amazon_order_id)
            
            # ! LOG DETAILED ERROR FOR DEBUGGING
            frappe.log_error(
                message=f"Error processing order {amazon_order_id}: {str(e)}\n{traceback.format_exc()}",
                title=f"Order Processing Error - {amazon_order_id}"
            )
    
    # Provide a summary if multiple orders were processed
    if len(orders) > 0:
        summary = []
        if created_orders:
            summary.append(f"Created {len(created_orders)} new orders.")
        if skipped_orders:
            summary.append(f"Skipped {len(skipped_orders)} existing orders.")
        if error_orders:
            summary.append(f"Failed to process {len(error_orders)} orders.")
        
        if summary:
            indicator = "green"
            if error_orders:
                indicator = "red"
            elif not created_orders:
                indicator = "blue"
                
            frappe.msgprint(
                msg=" ".join(summary),
                title="Order Sync Complete",
                indicator=indicator
            )
    
    return created_orders

@frappe.whitelist()
def order_does_not_exists(order):
    """Check if order already exists in ERPNext."""
    try:
        # ? PREVENT DUPLICATE ORDERS
        existing_order = frappe.db.exists(
            "Sales Order", {"custom_amazon_order_id": order["purchaseOrderNumber"]}
        )
        return not existing_order
    except KeyError:
        # ! MISSING REQUIRED ORDER ID
        frappe.log_error(
            message=f"Order missing purchaseOrderNumber: {frappe.as_json(order)}",
            title="Order Validation Error"
        )
        return False
    except Exception as e:
        # ! UNEXPECTED ERROR CHECKING ORDER EXISTENCE
        frappe.log_error(
            message=f"Error checking if order exists: {str(e)}\n{traceback.format_exc()}",
            title="Order Validation Error"
        )
        return False


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
    if not address_code:
        # ! CRITICAL ERROR - MISSING ADDRESS CODE
        frappe.log_error(
            message="No address code provided",
            title="Missing Address Code"
        )
        raise frappe.ValidationError("Missing address code for customer lookup")
        
    try:
        address = frappe.db.get_value(
            "Address", filters={"address_title": address_code}, fieldname=["name"]
        )

        if not address:
            # ! ADDRESS NOT FOUND - LOG DETAILED ERROR
            frappe.log_error(
                message=f"Address not found for code: {address_code}",
                title="Address Lookup Error"
            )
            raise frappe.ValidationError(f"Address not found for code: {address_code}")

        company = frappe.db.get_value(
            "Dynamic Link", filters={"parent": address}, fieldname=["link_title"]
        )

        if not company:
            # Log missing customer information
            frappe.log_error(
                message=f"No customer found for party ID: {address_code}",
                title="Missing Customer for Amazon Order"
            )
            raise frappe.ValidationError(f"Customer not linked to address: {address_code}")

        return {'address': address, 'company': company}
        
    except frappe.ValidationError:
        # Re-raise validation errors
        raise
    except Exception as e:
        # ! UNEXPECTED ERROR GETTING CUSTOMER
        frappe.log_error(
            message=f"Error getting customer from address {address_code}: {str(e)}\n{traceback.format_exc()}",
            title="Customer Lookup Error"
        )
        raise frappe.ValidationError(f"Error retrieving customer data: {str(e)}")


def get_default_warehouse():
    """
    Get default warehouse from Stock Settings.
    
    Returns:
        str: Default warehouse name
    """
    try:
        default_warehouse = frappe.db.get_single_value('Stock Settings', 'default_warehouse')
        if not default_warehouse:
            # ! MISSING REQUIRED CONFIGURATION
            frappe.log_error(
                message="Default warehouse not set in Stock Settings",
                title="Configuration Error"
            )
            frappe.throw("Default warehouse not set in Stock Settings")
        return default_warehouse
    except Exception as e:
        # ! CRITICAL ERROR - WAREHOUSE CONFIGURATION ISSUE
        frappe.log_error(
            message=f"Error getting default warehouse: {str(e)}\n{traceback.format_exc()}",
            title="Warehouse Configuration Error"
        )
        frappe.throw(f"Error retrieving default warehouse: {str(e)}")


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
    amazon_order_id = order.get("purchaseOrderNumber", "Unknown")
    log_title = f"Amazon Order {amazon_order_id}"
    
    try:
        # * Check for single item order early to avoid unnecessary processing
        items = order.get("orderDetails", {}).get("items", [])
        
        # If there are no items, log error and bail early
        if not items:
            error_msg = f"No items found in order data: {frappe.as_json(order)}"
            frappe.log_error(message=error_msg, title=log_title)
            frappe.msgprint(
                msg=f"Order {amazon_order_id} not created: No items found in order data.",
                title="Order Creation Failed",
                indicator="red"
            )
            return None
            
        # ? Check if this is a single-item order
        is_single_item_order = len(items) == 1
        
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
        try:
            customer_data = get_customer_from_address(address_code)
            sales_order.customer = customer_data.get('company')
            sales_order.customer_address = customer_data.get('address')
        except Exception as e:
            error_msg = f"Failed to get customer data for address code {address_code}: {str(e)}"
            frappe.log_error(message=error_msg, title=log_title)
            frappe.msgprint(
                msg=f"Order {amazon_order_id} not created: Customer lookup failed.",
                title="Order Creation Failed",
                indicator="red"
            )
            raise frappe.ValidationError(f"Customer lookup failed for Amazon Order {amazon_order_id}")
        
        sales_order.custom_amazon_order_id = amazon_order_id
        sales_order.custom_sales_person = sales_person
        sales_order.order_type = "Sales"
        
        try:
            company = get_default_company()
            sales_order.company = company.default_company
            sales_order.currency = company.default_currency
        except Exception as e:
            error_msg = f"Failed to get default company settings: {str(e)}"
            frappe.log_error(message=error_msg, title=log_title)
            frappe.msgprint(
                msg=f"Order {amazon_order_id} not created: Company settings error.",
                title="Order Creation Failed",
                indicator="red"
            )
            raise frappe.ValidationError(f"Company settings error for Amazon Order {amazon_order_id}")

        sales_order.delivery_date = delivery_date

        # * GET DEFAULT WAREHOUSE
        try:
            default_warehouse = get_default_warehouse()
        except Exception as e:
            error_msg = f"Failed to get default warehouse: {str(e)}"
            frappe.log_error(message=error_msg, title=log_title)
            frappe.msgprint(
                msg=f"Order {amazon_order_id} not created: Warehouse setting error.",
                title="Order Creation Failed",
                indicator="red"
            )
            raise frappe.ValidationError(f"Warehouse setting error for Amazon Order {amazon_order_id}")

        # * Set up taxes
        try:
            set_tax_and_charges_table(sales_order=sales_order)
        except Exception as e:
            error_msg = f"Failed to set tax and charges: {str(e)}"
            frappe.log_error(message=error_msg, title=log_title)
            frappe.msgprint(
                msg=f"Order {amazon_order_id} not created: Tax configuration error.",
                title="Order Creation Failed",
                indicator="red"
            )
            raise frappe.ValidationError(f"Tax configuration error for Amazon Order {amazon_order_id}")

        # * PROCESS ORDER ITEMS
        valid_items_count = 0
        
        for item in items:
            amazon_product_id = item.get("amazonProductIdentifier")
            if not amazon_product_id:
                frappe.log_error(
                    message=f"Missing amazonProductIdentifier in item: {frappe.as_json(item)}",
                    title=log_title
                )
                continue
                
            try:
                item_code, uom = get_item_code(amazon_product_id)

                if not item_code:  # ? Skip if item_code is not found
                    missing_vendor_items.append(amazon_product_id)
                    continue

                item_qty = item.get("orderedQuantity", {}).get("amount", 0)
                item_rate = item.get("netCost", {}).get("amount", 0)
                
                if not item_qty:
                    frappe.log_error(
                        message=f"Zero or missing quantity for item {amazon_product_id}",
                        title=log_title
                    )
                
                sales_order.append(
                    "items",
                    {
                        "item_code": item_code,
                        "delivery_date": delivery_date,
                        "qty": item_qty,
                        "rate": item_rate,
                        "uom": uom or "NOS",  # ? USE FETCHED UOM, DEFAULT TO "NOS" IF NONE
                        "warehouse": default_warehouse  # ? SET DEFAULT WAREHOUSE FROM STOCK SETTINGS
                    },
                )
                valid_items_count += 1

            except Exception as e:
                # Log detailed item error but continue processing other items
                error_msg = f"Error processing item {amazon_product_id}: {str(e)}\n{traceback.format_exc()}"
                frappe.log_error(message=error_msg, title=log_title)
        
        # ? Special handling for single-item orders where the item is not found
        if is_single_item_order and valid_items_count == 0:
            single_item = items[0]
            amazon_product_id = single_item.get("amazonProductIdentifier", "Unknown")
            
            error_msg = f"Cannot create order with single item that was not found: {amazon_product_id}"
            frappe.log_error(message=error_msg, title=log_title)
            
            # Show an error message on the screen
            frappe.msgprint(
                msg=f"Order {amazon_order_id} not created: Item '{amazon_product_id}' not found in the system.",
                title="Order Creation Failed",
                indicator="red"
            )
            
            return None  # Return None to indicate no sales order was created
        
        # ? Check if any valid items were added for multi-item orders
        if not sales_order.items:
            frappe.log_error(
                message=f"No valid items could be processed for order",
                title=log_title
            )
            frappe.msgprint(
                msg=f"Order {amazon_order_id} not created: No valid items found.",
                title="Order Creation Failed", 
                indicator="red"
            )
            return None

        # * SAVE AND COMMIT SALES ORDER
        try:
            sales_order.save()
            frappe.db.commit()
        except Exception as e:
            error_msg = f"Failed to save sales order: {str(e)}\n{traceback.format_exc()}"
            frappe.log_error(message=error_msg, title=log_title)
            frappe.db.rollback()
            frappe.msgprint(
                msg=f"Order {amazon_order_id} not created: Database error while saving.",
                title="Order Creation Failed",
                indicator="red"
            )
            raise frappe.ValidationError(f"Database error saving Amazon Order {amazon_order_id}")

        # * LOG MISSING VENDOR IDS IN SALES ORDER ITEM TRACKING DOCTYPE
        if missing_vendor_items and sales_order.name:
            try:
                tracking_doc = frappe.new_doc("Sales Order Item Tracking")
                tracking_doc.sales_order = sales_order.name
                tracking_doc.old_items = {'Missing Items': missing_vendor_items}  # JSON FIELD

                # Log before saving
                frappe.log_error(
                    message=f"Missing items for order {amazon_order_id}: {missing_vendor_items}",
                    title=f"{log_title} - Missing Items"
                )

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
            except Exception as e:
                # Just log this error but don't stop the process since the sales order is already created
                error_msg = f"Failed to create tracking record for missing items: {str(e)}\n{traceback.format_exc()}"
                frappe.log_error(message=error_msg, title=log_title)

        return sales_order.name

    except frappe.ValidationError:
        # Re-raise validation errors as they already contain our custom messages
        raise
        
    except Exception as e:
        # Catch all other unexpected errors
        error_msg = f"Unexpected error creating sales order: {str(e)}\n{traceback.format_exc()}"
        frappe.log_error(message=error_msg, title=log_title)
        frappe.msgprint(
            msg=f"Order {amazon_order_id} not created: Unexpected error occurred.",
            title="Order Creation Failed",
            indicator="red"
        )
        raise frappe.ValidationError(f"Failed to create sales order for Amazon Order {amazon_order_id}. Check logs for details.")
        
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


