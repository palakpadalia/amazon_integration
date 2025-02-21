#!/usr/bin/env python
# -*- coding: utf-8 -*-

# type: ignore[import]
import requests
import urllib.parse
import frappe
import datetime
from erpnext.controllers.accounts_controller import get_taxes_and_charges

#! CRITICAL: Never commit these credentials to version control
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
        #? Uses OAuth 2.0 token endpoint
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
        #! Critical error - without token, no API access possible
        frappe.log_error(str(e), "Access Token Error")
        raise


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
        #* Construct URL with proper encoding
        response = requests.get(
            f"{endpoint}/vendor/orders/v1/purchaseOrders?"
            + urllib.parse.urlencode(request_params),
            headers={"x-amz-access-token": access_token},
        )
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        #? Return empty orders list as fallback
        frappe.log_error(str(e), "Fetch Orders Error")
        return {"payload": {"orders": []}}


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
    #* Get API credentials and settings
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

    #? Early return if integration is disabled
    enabled = credentials["enable"]
    if not enabled:
        return

    #* Extract credentials
    refresh_token = credentials["refresh_token"]
    lwa_app_id = credentials["lwa_app_id"]
    lwa_client_secret = credentials["lwa_client_secret"]
    marketplace_id = credentials["marketplace_id"]
    endpoint = credentials["endpoint"]
    sales_person = credentials["amazon_sales_person"]

    #! Critical: Get fresh access token for API access
    access_token = get_access_token(refresh_token, lwa_app_id, lwa_client_secret)

    #? Default to last 2 hours if no start date provided
    if not created_after:
        created_after = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=2)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

    #* Prepare request parameters
    request_params = {
        "MarketplaceIds": marketplace_id,
        "createdAfter": created_after,
        "purchaseOrderState": "Acknowledged",
    }

    if created_before:
        request_params["createdBefore"] = created_before

    #* Fetch and process orders
    orders = get_orders(endpoint, request_params, access_token)
    orders_list = orders.get("payload", {}).get("orders", [])
    add_orders(orders_list, sales_person)

    return orders_list


@frappe.whitelist()
def add_orders(orders, sales_person):
    """Process multiple orders and create sales orders for new ones."""
    #* Loop through orders and create if not existing
    for order in orders:
        if order_does_not_exists(order):
            create_sales_order(order, sales_person)


@frappe.whitelist()
def order_does_not_exists(order):
    """Check if order already exists in ERPNext."""
    #? Prevent duplicate orders
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
        str: Company name
    """
    #* Two-step lookup: address -> company
    address = frappe.db.get_value(
        "Address", filters={"address_title": address_code}, fieldname=["name"]
    )

    company = frappe.db.get_value(
        "Dynamic Link", filters={"parent": address}, fieldname=["link_title"]
    )
    return {
        'address': address
        ,'company': company} 


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

        #* Extract and validate delivery date
        date_range = order.get("orderDetails", {}).get("deliveryWindow", "")
        delivery_date = None
        if date_range and "--" in date_range:
            try:
                delivery_date = date_range.split("--")[1].split("T")[0]
            except (IndexError, AttributeError):
                delivery_date = order.get("orderDetails", {}).get("purchaseOrderDate", "").split("T")[0]
        
        #? Fallback dates if not found
        if not delivery_date:
            delivery_date = order.get("orderDetails", {}).get("purchaseOrderDate", "").split("T")[0]

        if not delivery_date:
            delivery_date = frappe.utils.today()

        #* Set order header details
        sales_order.transaction_date = (
            order.get("orderDetails", {}).get("purchaseOrderDate", "").split("T")[0]
            or frappe.utils.today()
        )

        address_code = (
            order.get("orderDetails", {}).get("buyingParty", {}).get("partyId", "")
        )
        sales_order.customer = get_customer_from_address(address_code).get('company')
        sales_order.customer_address = get_customer_from_address(address_code).get('address')
        sales_order.custom_amazon_order_id = order.get("purchaseOrderNumber", "")

        sales_order.custom_sales_person = sales_person
        sales_order.order_type = "Sales"
        company = get_default_company()
        sales_order.company = company.default_company
        sales_order.currency = company.default_currency

        sales_order.delivery_date = delivery_date

        #* Get default warehouse
        default_warehouse = get_default_warehouse()

        #* Set up taxes
        set_tax_and_charges_table(sales_order=sales_order)

        #* Process order items
        items = order.get("orderDetails", {}).get("items", [])
        for item in items:
            amazon_product_id = item.get("amazonProductIdentifier")
            try:
                item_code = get_item_code(amazon_product_id)
                sales_order.append(
                    "items",
                    {
                        "item_code": item_code,
                        "delivery_date": delivery_date,
                        "qty": int(item.get("orderedQuantity", {}).get("amount", 0)),
                        "rate": float(item.get("netCost", {}).get("amount", 0)),
                        "uom": "Nos",
                        "warehouse": default_warehouse  # Set default warehouse from Stock Settings
                    },
                )
            except frappe.ValidationError as e:
                #! Critical: Log item processing errors
                error_msg = f"Error processing order {order.get('purchaseOrderNumber', '')}: {str(e)}"
                frappe.log_error(message=error_msg, title="Amazon Order Item Error")
                raise frappe.ValidationError(error_msg)

        #* Save and commit
        sales_order.save()
        frappe.db.commit()

        return sales_order.name

    except Exception as e:
        #! Critical: Log order creation errors
        frappe.log_error(message=str(e), title="Create Sales Order Error")
        raise


@frappe.whitelist()
def get_tax_and_charges_template():
    """Get default tax template."""
    #* Get default tax template
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
def get_item_code(item_code):
    """
    Get ERPNext item code from Amazon vendor ID.
    
    Raises:
        ValidationError: If item not found
    """
    #? Check if item exists
    found_item_code = frappe.db.get_value(
        "Item", filters={"custom_amazon_vendor_id": item_code}, fieldname="name"
    )
    if not found_item_code:
        #! Critical: Item not found
        frappe.throw(f"Item with Amazon Vendor ID '{item_code}' not found in the system")
    return found_item_code


def set_tax_and_charges_table(sales_order):
    """Set up tax and charges in sales order."""
    #* Get and apply tax template
    tax_and_charges_template = get_tax_and_charges_template()
    master_name = tax_and_charges_template["name"]
    tax_category = tax_and_charges_template["tax_category"]

    sales_order.tax_category = tax_category
    sales_order.taxes_and_charges = master_name

    #* Apply tax entries
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
    #? Set custom naming format for Amazon orders
    if doc.get("custom_amazon_order_id"):
        doc.name = f"AMZ-{doc.custom_amazon_order_id}"