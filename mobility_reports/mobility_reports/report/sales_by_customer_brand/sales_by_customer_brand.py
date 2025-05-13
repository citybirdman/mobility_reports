# Copyright (c) 2025, Aerele Technologies Private Limited and contributors
# For license information, please see license.txt
import frappe
import pandas as pd


def execute(filters=None):
	data=get_data(filters)
	columns = get_columns(data)
	return columns, data

def get_columns(data):
	columns = [
		{"fieldname": "customer", "label": "Customer", "fieldtype": "Data", "width": 200},
		{"fieldname": "customer_name", "label": "Customer Name", "fieldtype": "Data", "width": 200},
		{"fieldname": "brand", "label": "Brand", "fieldtype": "Data", "width": 200},
	]
	# Dynamically generate columns based on available years in the dataset.
	fileds = {key for row in data for key in row.keys() if key not in("customer", "customer_name", "brand")}
	# Add dynamic year columns
	for filed in fileds:
		columns.append({"fieldname": filed, "label": f"Qty ({filed})", "fieldtype": "Int", "width": 200})

	return columns
def get_data(filters):
	from_date = filters.get('from_date')
	to_date = filters.get('to_date')
	data = frappe.db.sql('''
		WITH sales_table AS (
			SELECT name, customer, customer_name FROM `tabSales Invoice`
			UNION ALL
			SELECT name, customer, customer_name FROM `tabDelivery Note`
		)
		SELECT 
			DATE_FORMAT(sle.posting_date, '%%Y-%%m') AS date,
			st.customer,
			st.customer_name,
			i.brand,
			ROUND(SUM(sle.actual_qty) * -1, 0) AS qty
		FROM `tabStock Ledger Entry` sle
		JOIN sales_table st ON sle.voucher_no = st.name
		JOIN `tabItem` i ON sle.item_code = i.name
		WHERE sle.voucher_type IN ('Sales Invoice', 'Delivery Note')
			AND sle.docstatus = 1 
			AND sle.is_cancelled = 0
			AND sle.posting_date BETWEEN %(from_date)s AND %(to_date)s
		GROUP BY date, customer, customer_name, brand
	''', {
		'from_date': from_date,
		'to_date': to_date
	}, as_dict=True)
	
	#create pivot table with date column be the index and customer,customer_name,brand as columns
	data = [dict(row) for row in data]

	df = pd.DataFrame(data)

	sales_pivot=df.pivot_table(
		values='qty',
		columns=['date'],
		index=['customer','customer_name','brand'],
		aggfunc='sum',
		fill_value=0
	).reset_index()

	return sales_pivot.to_dict(orient='records')


