// Copyright (c) 2024, Palak P and contributors
// For license information, please see license.txt

frappe.ui.form.on("Amazon Settings", {
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
        console.log(`To Date (ISO): ${created_before}`);
        
        // Show loading overlay
        frappe.dom.freeze('Fetching orders from Amazon...');
        
        // Call the server-side method
        frappe.call({
            method: 'amazon_integration.amazon_integration.py.amazon.sync_amazon_vendor_orders',
            args: {
                created_after: created_after,
                created_before: created_before
            },
            callback: (res) => {
                // Hide loading overlay
                frappe.dom.unfreeze();
                
                if (res.message && Array.isArray(res.message) && res.message.length > 0) {
                    frappe.msgprint({
                        title: __('Success'),
                        message: __(`Amazon orders synced successfully.`),
                        indicator: 'green'
                    });
                    console.log('Response:', res.message);
                } else {
                    frappe.msgprint({
                        title: __('Information'),
                        message: __('No new orders were found to sync.'),
                        indicator: 'yellow'
                    });
                }
            },
            error: (err) => {
                // Hide loading overlay
                frappe.dom.unfreeze();
                
                console.error('Error syncing orders:', err);
                frappe.msgprint({
                    title: __('Error'),
                    message: __('Failed to sync orders. Please try again or contact support if the issue persists.'),
                    indicator: 'red'
                });
            }
        });
    },
});