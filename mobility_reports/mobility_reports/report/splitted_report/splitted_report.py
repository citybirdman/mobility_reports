# Copyright (c) 2025, Aerele Technologies Private Limited and contributors
# For license information, please see license.txt

import frappe

def execute(filters=None):
    data = {
        "sales_orders": get_sales_orders(filters),
        "sales_invoices": get_sales_invoices(filters)
    }
    
    return data

def get_sales_orders(filters):
    return frappe.db.sql("""
        SELECT 
            name, customer, total 
        FROM `tabSales Order`
        WHERE docstatus = 1
    """, as_dict=True)

def get_sales_invoices(filters):
    return frappe.db.sql("""
        SELECT 
            name, customer, grand_total 
        FROM `tabSales Invoice`
        WHERE docstatus = 1
    """, as_dict=True)

