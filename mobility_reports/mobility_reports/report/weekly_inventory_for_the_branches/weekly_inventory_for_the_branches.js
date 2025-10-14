// Copyright (c) 2025, Aerele Technologies Private Limited and contributors
// For license information, please see license.txt

frappe.query_reports["Weekly Inventory For The Branches"] = {
	"filters": [
		{
			"fieldname": "warehouse",
			"label": "Warehouse",
			"fieldtype": "Select",
			"options": ["Jeddah", "Dammam Main", "Riyadh"],
			"reqd": 1
		},

		{
			"fieldname": "brand",
			"label": "Brand",
			"fieldtype": "MultiSelect",
			"options": ['ROADSTONE', 'LANDSAIL-TH', 'DOUBLESTAR', 'DYNAMO', 'DOUBLESTAR-TBR', 'KENDA-CN'],
			"reqd": 1
		},
		
		{
			"fieldname": "qty",
			"label": "Quantity Greter Than ",
			"fieldtype": "Int",
			"default":60
		}
	]
};

