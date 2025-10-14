// Copyright (c) 2025, Aerele Technologies Private Limited and contributors
// For license information, please see license.txt

frappe.query_reports["Sales GM"] = {
	"filters": [
		{
			"fieldname": "metric",
			"label": "Metric",
			"fieldtype": "Select",
			"options": ['Location Zone','Items'],
			"fieldtype": "Select",
			"default": "Items",
			"reqd": 1
		}
		

	]
};
