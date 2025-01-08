import requests
import urllib.parse
import frappe
import datetime

# Function to get the access token using refresh token, LWA app ID, and client secret
def get_access_token(refresh_token, lwa_app_id, lwa_client_secret):
    try:
        # Make a POST request to fetch the access token
        token_response = requests.post(
            "https://api.amazon.com/auth/o2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": lwa_app_id,
                "client_secret": lwa_client_secret,
            },
        )
        # Raise an error if the request fails
        token_response.raise_for_status()

        # Extract and return the access token from the response
        return token_response.json().get("access_token")

    except requests.exceptions.RequestException as e:
        # Log errors for debugging
        frappe.log_error(str(e), "Access Token Error")
        raise


# Function to fetch orders from Amazon Vendor API
def get_orders(endpoint, request_params, access_token):
    try:
        # Make a GET request to fetch orders
        response = requests.get(
            f"{endpoint}/vendor/orders/v1/purchaseOrders?" + urllib.parse.urlencode(request_params),
            headers={"x-amz-access-token": access_token},
        )
        # Raise an error if the request fails
        response.raise_for_status()

        # Return the parsed JSON data
        return response.json()

    except requests.exceptions.RequestException as e:
        # Log errors for debugging
        frappe.log_error(str(e), "Fetch Orders Error")
        return {"payload": {"orders": []}}


# Function to synchronize Amazon vendor orders with the local system
@frappe.whitelist()
def sync_amazon_vendor_orders(created_after=None, created_before = None):
    # Fetch credentials for Amazon Seller API
    credentials = get_credentials(
        'Amazon API Settings',
        fields=['refresh_token', 'lwa_app_id', 'lwa_client_secret', 'endpoint', 'marketplace_id','amazon_sales_person']
    )

    refresh_token = credentials['refresh_token']
    lwa_app_id = credentials['lwa_app_id']
    lwa_client_secret = credentials['lwa_client_secret']
    marketplace_id = credentials['marketplace_id']
    endpoint = credentials['endpoint']
    sales_person = credentials['amazon_sales_person']

    # Get the LWA access token
    access_token = get_access_token(refresh_token, lwa_app_id, lwa_client_secret)

    # Set default created_after value if not provided
    if not created_after:
        created_after = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=2)
        ).strftime('%Y-%m-%dT%H:%M:%SZ')

    # Prepare request parameters for fetching orders
    request_params = {
        "MarketplaceIds": marketplace_id,
        "createdAfter": created_after,
        'purchaseOrderState' : 'Acknowledged',
    }

    if created_before:
        request_params["createdBefore"] = created_before
    # Fetch orders and handle response data
    orders = get_orders(endpoint, request_params, access_token)
    orders_list = orders.get("payload", {}).get("orders", [])

    # Add orders to the local database or process them
    # try:
    add_orders(orders_list, sales_person)

    # except Exception as e:
    #     frappe.log_error(message=str(e), title="Error Adding Orders")

    return orders_list


# Function to add orders to the local database
@frappe.whitelist()
def add_orders(orders,sales_person):
    for order in orders:
        # Check if the order does not exist before creating it
        if order_does_not_exists(order):
            create_sales_order(order,sales_person)


# Function to check if an order already exists
@frappe.whitelist()
def order_does_not_exists(order):
    # Check for existing sales orders by custom Amazon order ID
    existing_order = frappe.db.exists('Sales Order', {'custom_amazon_order_id': order['purchaseOrderNumber']})
    return not existing_order

@frappe.whitelist()
def get_customer_from_address(address_code):
    address = frappe.db.get_value(
        'Address',
        filters= {
            'address_title': address_code
        },
        fieldname= ['name']
    )

    company = frappe.db.get_value(
        'Dynamic Link',
        filters= {
            'parent': address
        },
        fieldname= ['link_title']
    )
    return company

# Function to create a new sales order in the system
@frappe.whitelist()
def create_sales_order(order,sales_person):
    try:
        sales_order = frappe.new_doc('Sales Order')
        # Extract and set fields from the order data
        date_range = order.get('orderDetails', {}).get('deliveryWindow', '')
        delivery_date = date_range.split("--")[1].split("T")[0] if date_range else None
        sales_order.transaction_date = order.get('orderDetails', {}).get('purchaseOrderDate', '').split('T')[0]
        address_code = order.get('orderDetails', {}).get('buyingParty', {}).get('partyId', '')
        sales_order.customer = get_customer_from_address(address_code)
        sales_order.custom_amazon_order_id = order.get('purchaseOrderNumber', '')
        print(order)
        # Set default fields
        sales_order.custom_sales_person = sales_person
        sales_order.order_type = 'Sales'
        company = get_default_company()
        sales_order.company = company.default_company
        sales_order.currency = company.default_currency
        sales_order.selling_price_list = 'Standard Selling'

        sales_order.taxes_and_charges = get_tax_and_charges_template()
        # Add items to the sales order
        items = order.get('orderDetails', {}).get('items', [])
        for item in items:
            item_code = get_item_code(item.get('amazonProductIdentifier')) or '6291106599046'
            sales_order.append('items', {

                'item_code': item_code,
                'delivery_date': delivery_date,
                'qty': int(item.get('orderedQuantity', {}).get('amount', 0)),
                'rate': float(item.get('netCost', {}).get('amount', 0)),
                'uom': 'Nos'
            })

        
        # Save and submit the sales order
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
        'Sales Taxes and Charges Template',
        filters={'is_default': 1},
        fieldname='name'
    )
    return template

def get_default_company():

    company = frappe.get_doc(
        'Global Defaults',
    )
    return company

# Function to get system credentials for a given doctype
@frappe.whitelist()
def get_credentials(doctype, fields):
    # Fetch the document and retrieve the specified fields
    doc = frappe.get_doc(doctype)
    credentials = {field: getattr(doc, field, None) for field in fields}
    return credentials


# Function to get the item code based on Amazon vendor product ID
@frappe.whitelist()
def get_item_code(item_code):
    return frappe.db.get_value(
        'Item',
        filters={'custom_amazon_vendor_id': item_code},
        fieldname='name'
    )



def autoname(doc, method):
    """
    Custom autoname method for Sales Order.
    This method sets the naming convention based on Amazon Integration.
    """

    # Check if the Sales Order is related to Amazon
    if doc.get('custom_amazon_order_id'):
        # Generate the name using a custom pattern
       doc.name = f"AMZ-{doc.custom_amazon_order_id}"

