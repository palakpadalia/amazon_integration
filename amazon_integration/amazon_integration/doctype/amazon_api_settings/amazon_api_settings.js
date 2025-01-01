// Copyright (c) 2024, Palak P and contributors
// For license information, please see license.txt

frappe.ui.form.on("Amazon API Settings", {
    load_sync(frm) {
        // Convert to ISO 8601 format with 'Z' timezone
        const formatToISO8601 = (dateString) => {
            if (!dateString) return null;
            const date = new Date(dateString);
            return date.toISOString().split('.')[0] + 'Z'; // Ensures format is 'YYYY-MM-DDTHH:mm:ssZ'
        };

        // Get and format the dates
        const created_after = formatToISO8601(frm.doc.from_date);
        const created_before = formatToISO8601(frm.doc.to_date);

        console.log(`From Date (ISO): ${created_after}`);
        console.log(`From Date (ISO): ${created_before}`);
        // console.log(`To Date (ISO): ${created_before}`);

        // Call the server-side method
        frappe.call({
            method: 'amazon_integration.amazon_integration.py.amazon.sync_amazon_vendor_orders',
            args: {
                // created_before: created_before,
                created_after: created_after,
                created_before: created_before
            },
            callback: (res) => {
                if (res.message) {
                    frappe.msgprint({
                        title: __('Success'),
                        message: __('Orders synced successfully.'),
                        indicator: 'green'
                    });
                    console.log('Response:', res.message);
                } else {
                    frappe.msgprint({
                        title: __('Error'),
                        message: __('No orders were synced.'),
                        indicator: 'red'
                    });
                }
            }
        });
    },
});


