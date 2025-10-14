// Copyright (c) 2025, Aerele Technologies Private Limited and contributors
// For license information, please see license.txt

frappe.query_reports["Multi-Dimensional Sales Breakdown"] = {
	"filters": [
		{
			"fieldname": "metric",
			"label": "Metric",
			"fieldtype": "Select",
			"options": ['Item (Qty)', 'Item (Amount)', 'Sales Channel (Qty)', 'Location Zone (Qty)', 'Country (Qty)'],
			"default": "Item (Qty)"
		}

	]
};
