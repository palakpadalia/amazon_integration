import requests
import urllib.parse
import frappe
import datetime

# Function to get the access token using refresh token, LWA app ID, and client secret
def get_access_token(refresh_token, lwa_app_id, lwa_client_secret):
    # Send a POST request to Amazon's token endpoint to get the access token
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
        token_response.raise_for_status()  # Raise an error if the response status is not 200
        return token_response.json().get("access_token")  # Return the access token from the response

    # Handle errors and log them for debugging
    except requests.exceptions.RequestException as e:
        frappe.log_error(str(e), "Access Token Error")
        raise

# Function to fetch orders from Amazon Vendor API
def get_orders(endpoint, request_params, access_token):
    # Send a GET request to fetch orders from the Amazon Vendor API
    try:
        response = requests.get(
            f"{endpoint}/vendor/orders/v1/purchaseOrders?" + urllib.parse.urlencode(request_params),
            headers={"x-amz-access-token": access_token},
        )
        response.raise_for_status()  # Raise an error if the response status is not 200
        return response.json()  # Return the JSON response containing order details

    # Log errors and return an empty order list if an error occurs
    except requests.exceptions.RequestException as e:
        frappe.log_error(str(e), "Fetch Orders Error")
        return {"payload": {"orders": []}}

# Function to synchronize Amazon vendor orders with the local system
@frappe.whitelist()
def sync_amazon_vendor_orders(created_after=None, created_before=None):
    # Retrieve API credentials and configuration
    credentials = get_credentials(
        'Amazon Vendor Settings',
        fields=['refresh_token', 'lwa_app_id', 'lwa_client_secret', 'endpoint', 'marketplace_id', 'amazon_sales_person', 'enable']
    )
    
    enabled = credentials['enable']
    if not enabled:  # Exit if the integration is disabled
        return
    
    # Extract credentials and API settings
    refresh_token = credentials['refresh_token']
    lwa_app_id = credentials['lwa_app_id']
    lwa_client_secret = credentials['lwa_client_secret']
    marketplace_id = credentials['marketplace_id']
    endpoint = credentials['endpoint']
    sales_person = credentials['amazon_sales_person']

    # Get a valid access token
    access_token = get_access_token(refresh_token, lwa_app_id, lwa_client_secret)

    # Set default createdAfter date to 2 hours ago if not provided
    if not created_after:
        created_after = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=2)
        ).strftime('%Y-%m-%dT%H:%M:%SZ')

    # Prepare request parameters for the API
    request_params = {
        "MarketplaceIds": marketplace_id,
        "createdAfter": created_after,
        "purchaseOrderState": "Acknowledged",
    }

    if created_before:  # Add createdBefore parameter if provided
        request_params["createdBefore"] = created_before

    # Fetch orders from Amazon
    orders = get_orders(endpoint, request_params, access_token)
    orders_list = orders.get("payload", {}).get("orders", [])
    add_orders(orders_list, sales_person)  # Add fetched orders to the local system

    return orders_list  # Return the list of orders

# Function to add orders to the local database
@frappe.whitelist()
def add_orders(orders, sales_person):
    # Iterate through each order and create a Sales Order if it doesn't already exist
    for order in orders:
        if order_does_not_exists(order):  # Check if the order already exists
            create_sales_order(order, sales_person)  # Create a new Sales Order

# Function to check if an order already exists
@frappe.whitelist()
def order_does_not_exists(order):
    # Check if a Sales Order with the given Amazon order ID exists in the database
    existing_order = frappe.db.exists('Sales Order', {'custom_amazon_order_id': order['purchaseOrderNumber']})
    return not existing_order

# Function to get the customer linked to a specific address
@frappe.whitelist()
def get_customer_from_address(address_code):
    # Retrieve the address from the Address doctype using the address title
    address = frappe.db.get_value(
        'Address',
        filters={'address_title': address_code},
        fieldname=['name']
    )

    # Retrieve the customer linked to this address from the Dynamic Link table
    company = frappe.db.get_value(
        'Dynamic Link',
        filters={'parent': address},
        fieldname=['link_title']
    )
    return company

from erpnext.controllers.accounts_controller import get_taxes_and_charges

# Function to create a new sales order in the system
@frappe.whitelist()
def create_sales_order(order, sales_person):
    # Create a new Sales Order document in Frappe
    try:
        sales_order = frappe.new_doc('Sales Order')
        
        # Extract and set delivery date and transaction date from the order data
        date_range = order.get('orderDetails', {}).get('deliveryWindow', '')
        delivery_date = date_range.split("--")[1].split("T")[0] if date_range else None
        sales_order.transaction_date = order.get('orderDetails', {}).get('purchaseOrderDate', '').split('T')[0]
        
        # Retrieve the customer linked to the order address
        address_code = order.get('orderDetails', {}).get('buyingParty', {}).get('partyId', '')
        sales_order.customer = get_customer_from_address(address_code)
        sales_order.custom_amazon_order_id = order.get('purchaseOrderNumber', '')
        
        # Set default fields like sales person, company, and currency
        sales_order.custom_sales_person = sales_person
        sales_order.order_type = 'Sales'
        company = get_default_company()
        sales_order.company = company.default_company
        sales_order.currency = company.default_currency

        # Set tax and charges table for the sales order
        set_tax_and_charges_table(sales_order=sales_order)
        
        # Add items to the sales order
        items = order.get('orderDetails', {}).get('items', [])
        for item in items:
            item_code = get_item_code(item.get('amazonProductIdentifier')) or 'Turmeric Whole India-25kg'
            sales_order.append('items', {
                'item_code': item_code,
                'delivery_date': delivery_date,
                'qty': int(item.get('orderedQuantity', {}).get('amount', 0)),
                'rate': float(item.get('netCost', {}).get('amount', 0)),
                'uom': 'Nos'
            })

        sales_order.save()  # Save the Sales Order document
        sales_order.submit()  # Submit the Sales Order
        frappe.db.commit()  # Commit the transaction
        
        return sales_order.name

    # Log errors if any occur while creating the Sales Order
    except Exception as e:
        frappe.log_error(message=str(e), title="Create Sales Order Error")
        raise

# Function to get the default tax and charges template
@frappe.whitelist()
def get_tax_and_charges_template(): 
    # Retrieve the default tax and charges template marked as default
    template = frappe.db.get_value(
        'Sales Taxes and Charges Template',
        filters={'is_default': 1},
        fieldname=['name', 'tax_category'],
        as_dict=1
    )
    return template

# Function to get the default company settings
def get_default_company():
    # Retrieve the default company and currency from Global Defaults
    company = frappe.get_doc('Global Defaults')
    return company

# Function to get system credentials for a given doctype
@frappe.whitelist()
def get_credentials(doctype, fields):
    # Fetch credentials for the specified doctype and fields
    doc = frappe.get_doc(doctype)
    credentials = {field: getattr(doc, field, None) for field in fields}
    return credentials

# Function to get the item code based on Amazon vendor product ID
@frappe.whitelist()
def get_item_code(item_code):
    # Retrieve the item code using the custom Amazon vendor ID
    return frappe.db.get_value(
        'Item',
        filters={'custom_amazon_vendor_id': item_code},
        fieldname='name'
    )

# Function to set tax and charges table for the sales order
def set_tax_and_charges_table(sales_order):
    # Populate the taxes table in the sales order based on the tax template
    tax_and_charges_template = get_tax_and_charges_template()
    master_name = tax_and_charges_template['name']
    tax_category = tax_and_charges_template['tax_category']
    
    sales_order.tax_category = tax_category
    sales_order.taxes_and_charges = master_name

    # Fetch taxes and append them to the Sales Order
    tax_entries = get_taxes_and_charges(
        master_doctype='Sales Taxes and Charges Template',
        master_name=master_name
    )

    if tax_entries:
        for tax in tax_entries:
            sales_order.append('taxes', {
                'charge_type': tax.get('charge_type', 'On Net Total'),
                'account_head': tax.get('account_head'),
                'description': tax.get('description', ''),
                'rate': tax.get('rate', 0.0),
                'cost_center': tax.get('cost_center', ''),
                'included_in_print_rate': tax.get('included_in_print_rate', 0),
                'included_in_paid_amount': tax.get('included_in_paid_amount', 0),
            })

# Function for custom autoname logic
def autoname(doc, method):
    # Set the Sales Order name using the Amazon order ID
    if doc.get('custom_amazon_order_id'):
        doc.name = f"AMZ-{doc.custom_amazon_order_id}"
