// Copyright (c) 2025, Aerele Technologies Private Limited and contributors
// For license information, please see license.txt
// sssjjhj

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
			"fieldtype": "Link",
			"options": "Production Year",
			"reqd": 1
		},

		
		{
			"fieldname": "price_list",
			"label": __("Price List"),
			"fieldtype": "Link",
			"options": "Price List", // Changed from "Item Price" to "Price List"
			"reqd": 1,
			// CRITICAL: Filter to show only SELLING price lists
			"get_query": function() {
				return {
					"filters": {
						"selling": 1
					}
				};
			}
		}
	]
};
