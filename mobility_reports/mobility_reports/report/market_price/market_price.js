// Copyright (c) 2025, Aerele Technologies Private Limited and contributors
// For license information, please see license.txt

frappe.query_reports["Market Price"] = {
	"filters":[
		{
			"fieldname": "from_date",
			"label": "Date",
			"fieldtype": "Date",
			"reqd": 1
		},
		{
			"fieldname": "brand",
			"label": "Brand",
			"fieldtype": "Link",
			"options": "Brand",
			"reqd": 0
		},
		{
			"fieldname": "production_year",
			"label": "Production Year",
			"fieldtype": "Data"
		}
	]
};
