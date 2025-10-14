// Copyright (c) 2025, Aerele Technologies Private Limited and contributors
// For license information, please see license.txt


frappe.query_reports["Sales & Invoices Report"] = {
    onload: function(report) {
        report.page.add_inner_button("Sales Orders", function() {
            report.data = report.raw_data.sales_orders;
            report.refresh();
        });

        report.page.add_inner_button("Sales Invoices", function() {
            report.data = report.raw_data.sales_invoices;
            report.refresh();
        });
    },
    get_data: function(data) {
        return data;
    }
};

