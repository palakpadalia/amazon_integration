import requests
import urllib.parse
import frappe
import datetime
from erpnext.controllers.accounts_controller import get_taxes_and_charges


def get_access_token(refresh_token, lwa_app_id, lwa_client_secret):
    try:
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
        frappe.log_error(str(e), "Access Token Error")
        raise


def get_orders(endpoint, request_params, access_token):
    try:
        response = requests.get(
            f"{endpoint}/vendor/orders/v1/purchaseOrders?"
            + urllib.parse.urlencode(request_params),
            headers={"x-amz-access-token": access_token},
        )
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        frappe.log_error(str(e), "Fetch Orders Error")
        return {"payload": {"orders": []}}


@frappe.whitelist()
def sync_amazon_vendor_orders(created_after=None, created_before=None):
    credentials = get_credentials(
        "Amazon Vendor Settings",
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

    enabled = credentials["enable"]
    if not enabled:
        return

    refresh_token = credentials["refresh_token"]
    lwa_app_id = credentials["lwa_app_id"]
    lwa_client_secret = credentials["lwa_client_secret"]
    marketplace_id = credentials["marketplace_id"]
    endpoint = credentials["endpoint"]
    sales_person = credentials["amazon_sales_person"]

    access_token = get_access_token(refresh_token, lwa_app_id, lwa_client_secret)

    if not created_after:
        created_after = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=2)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

    request_params = {
        "MarketplaceIds": marketplace_id,
        "createdAfter": created_after,
        "purchaseOrderState": "Acknowledged",
    }

    if created_before:
        request_params["createdBefore"] = created_before

    orders = get_orders(endpoint, request_params, access_token)
    orders_list = orders.get("payload", {}).get("orders", [])
    add_orders(orders_list, sales_person)

    return orders_list


@frappe.whitelist()
def add_orders(orders, sales_person):
    for order in orders:
        if order_does_not_exists(order):
            create_sales_order(order, sales_person)


@frappe.whitelist()
def order_does_not_exists(order):
    existing_order = frappe.db.exists(
        "Sales Order", {"custom_amazon_order_id": order["purchaseOrderNumber"]}
    )
    return not existing_order


@frappe.whitelist()
def get_customer_from_address(address_code):
    address = frappe.db.get_value(
        "Address", filters={"address_title": address_code}, fieldname=["name"]
    )

    company = frappe.db.get_value(
        "Dynamic Link", filters={"parent": address}, fieldname=["link_title"]
    )
    return company


@frappe.whitelist()
def create_sales_order(order, sales_person):
    try:
        sales_order = frappe.new_doc("Sales Order")

        # Extract and validate delivery date
        date_range = order.get("orderDetails", {}).get("deliveryWindow", "")
        delivery_date = None
        if date_range and "--" in date_range:
            try:
                delivery_date = date_range.split("--")[1].split("T")[0]
            except (IndexError, AttributeError):
                delivery_date = order.get("orderDetails", {}).get("purchaseOrderDate", "").split("T")[0]
        
        if not delivery_date:
            delivery_date = order.get("orderDetails", {}).get("purchaseOrderDate", "").split("T")[0]

        if not delivery_date:
            delivery_date = frappe.utils.today()

        sales_order.transaction_date = (
            order.get("orderDetails", {}).get("purchaseOrderDate", "").split("T")[0]
            or frappe.utils.today()
        )

        address_code = (
            order.get("orderDetails", {}).get("buyingParty", {}).get("partyId", "")
        )
        sales_order.customer = get_customer_from_address(address_code)
        sales_order.custom_amazon_order_id = order.get("purchaseOrderNumber", "")

        sales_order.custom_sales_person = sales_person
        sales_order.order_type = "Sales"
        company = get_default_company()
        sales_order.company = company.default_company
        sales_order.currency = company.default_currency

        sales_order.delivery_date = delivery_date

        set_tax_and_charges_table(sales_order=sales_order)

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
                    },
                )
            except frappe.ValidationError as e:
                error_msg = f"Error processing order {order.get('purchaseOrderNumber', '')}: {str(e)}"
                frappe.log_error(message=error_msg, title="Amazon Order Item Error")
                raise frappe.ValidationError(error_msg)

        sales_order.save()
        sales_order.submit()
        frappe.db.commit()

        return sales_order.name

    except Exception as e:
        frappe.log_error(message=str(e), title="Create Sales Order Error")
        raise


@frappe.whitelist()
def get_tax_and_charges_template():
    template = frappe.db.get_value(
        "Sales Taxes and Charges Template",
        filters={"is_default": 1},
        fieldname=["name", "tax_category"],
        as_dict=1,
    )
    return template


def get_default_company():
    company = frappe.get_doc("Global Defaults")
    return company


@frappe.whitelist()
def get_credentials(doctype, fields):
    doc = frappe.get_doc(doctype)
    credentials = {field: getattr(doc, field, None) for field in fields}
    return credentials


@frappe.whitelist()
def get_item_code(item_code):
    found_item_code = frappe.db.get_value(
        "Item", filters={"custom_amazon_vendor_id": item_code}, fieldname="name"
    )
    if not found_item_code:
        frappe.throw(f"Item with Amazon Vendor ID '{item_code}' not found in the system")
    return found_item_code


def set_tax_and_charges_table(sales_order):
    tax_and_charges_template = get_tax_and_charges_template()
    master_name = tax_and_charges_template["name"]
    tax_category = tax_and_charges_template["tax_category"]

    sales_order.tax_category = tax_category
    sales_order.taxes_and_charges = master_name

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
    if doc.get("custom_amazon_order_id"):
        doc.name = f"AMZ-{doc.custom_amazon_order_id}"