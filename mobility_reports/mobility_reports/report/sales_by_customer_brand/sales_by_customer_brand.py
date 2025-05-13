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
			columns.append({"fieldname": filed, "label": filed, "fieldtype": "Float", "width": 200})	
	return columns
def get_data(filters):
	from_date = filters.get('from_date')
	to_date = filters.get('to_date')
	data = frappe.db.sql('''
				SELECT 
			DATE_FORMAT(si.posting_date, '%%Y-%%m') AS date,
			si.customer,
			si.customer_name,
			sii.brand,
			ROUND(SUM(sii.qty), 0) AS qty,
            ROUND(sum(sii.net_amount))as amount
		FROM `tabSales Invoice`as si
		JOIN `tabSales Invoice Item` sii ON si.name = sii.parent
			where si.docstatus = 1 
			AND si.posting_date BETWEEN %(from_date)s AND %(to_date)s
		GROUP BY date, customer, customer_name, brand
	''', {
		'from_date': from_date,
		'to_date': to_date
	}, as_dict=True)
	
	#create pivot table with date column be the index and customer,customer_name,brand as columns
	data = [dict(row) for row in data]

	df = pd.DataFrame(data)

	pivot = df.pivot_table(
		values=['qty', 'amount'],
		columns='date',
		index=['customer', 'customer_name', 'brand'],
		aggfunc='sum',
		fill_value=0
	)

	# Step 1: Flatten MultiIndex and format as 'amount(YYYY-MM)' or 'qty(YYYY-MM)'
	pivot.columns = [f"{metric}({date})" for metric, date in pivot.columns]

	# Step 2: Reset index to bring 'customer', etc., back as columns
	pivot.reset_index(inplace=True)

	# Step 3: Reorder columns - base fields first
	base_cols = ['customer', 'customer_name', 'brand']

	# Separate amount and qty columns and sort by date
	cols = [col for col in pivot.columns if 'amount' in col or 'qty' in col]

	# qty_cols = [col for col in pivot.columns if col.startswith('qty(')]

	ordered_cols = base_cols + cols
	pivot = pivot[ordered_cols]
	
	return pivot.to_dict('records')

	


