// Copyright (c) 2025, Aerele Technologies Private Limited and contributors
// For license information, please see license.txt

frappe.query_reports["Profit Summary by Category"] = {
	"filters": [

		{
			'fieldname':'quarterly',
			'label':'Quarterly',
			'fieldtype':'Select',
			'options': ['Q1','Q2','Q3','Q4'],
			'default':'Q1',
			'reqd':1		
		},
		{
			"fieldname": "category",
			"label": "Category",
			"fieldtype": "Select",
			"options": ['Summary','Item','Region'],
			'default':'Summary',
			'reqd':1		
		}
		

	]
};
