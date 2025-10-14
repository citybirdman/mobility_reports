// Copyright (c) 2025, Aerele Technologies Private Limited and contributors
// For license information, please see license.txt

frappe.query_reports["Monthly Comparing"] = {
	"filters": [
			{
			"fieldname": "aggregation_type",
			"label": "Aggregation Type",
			"fieldtype": "Select",
			"options": ["Category", "SKU", "Customer", "Sales Channel", "Location Zone"],
			"default": "Category",
			"reqd": 1
		},
		{
			"fieldname": "month",
			"label": "Month",
			"fieldtype": "Select",
			"options": [
				"January", "February", "March", "April", "May", "June",
				"July", "August", "September", "October", "November", "December"
			],
			"reqd": 1
		},
		{
			"fieldname": "value_type",
			"label": "Value Type",
			"fieldtype": "Select",
			"options": ["Qty", "Amount"],
			"reqd": 1
		}

	]
};
