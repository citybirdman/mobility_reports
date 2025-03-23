// Copyright (c) 2025, Aerele Technologies Private Limited and contributors
// For license information, please see license.txt

frappe.query_reports["Supplier Quantitative History"] = {
	"filters": [
		{
            "fieldname": "brand",
            "label": "Brand",
            "fieldtype": "Link",
            "options": "Brand", 
            "reqd": 0
        }
	]
};
