// Copyright (c) 2025, Aerele Technologies Private Limited and contributors
// For license information, please see license.txt

frappe.query_reports["Order Planning Summary"] = {
	"filters": [
		{
			"fieldname": "brand",
			"label": "Brand",
			"fieldtype": "Link",
			"options":"Brand",	
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
		},
		{
			"fieldname": "production_year",
			"label": "Production Year",
			"fieldtype": "Link",
			"options": "Production Year",
			"reqd": 1
		}
	]
};
