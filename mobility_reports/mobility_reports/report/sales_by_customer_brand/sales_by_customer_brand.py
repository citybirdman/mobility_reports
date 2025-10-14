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
		{"fieldname": "customer", "label": "Customer", "fieldtype": "Data", "width": 150},
		{"fieldname": "customer_name", "label": "Customer Name", "fieldtype": "Data", "width": 180},
		{"fieldname": "brand", "label": "Brand", "fieldtype": "Data", "width": 120},
	]

	if data:
		fieldnames = list(data[0].keys())[3:]  # Skip the first 3 base columns
		for key in fieldnames:
			if key.startswith("qty("):
				fieldtype = "Int"
			elif key.startswith("amount("):
				fieldtype = "Float"
			else:
				fieldtype = "Data"  
			columns.append({
				"fieldname": key,
				"label": key,
				"fieldtype": fieldtype,
				"width": 150
			})

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
			AND sii.income_account IN (
					  SELECT default_income_account from `tabCompany`
					  union all 
					  select default_sales_return_account from `tabCompany`
					  )
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

	months = sorted(pivot.columns.levels[1])

	ordered_cols = [('qty', m) for m in months] + [('amount', m) for m in months]
	ordered_cols = [col for pair in zip([('qty', m) for m in months], [('amount', m) for m in months]) for col in pair if col in pivot.columns]

	pivot = pivot[ordered_cols]
	pivot.columns = [f"{metric}({month})" for metric, month in pivot.columns]

	pivot.reset_index(inplace=True)
	pivot = pivot[['customer', 'customer_name', 'brand'] + pivot.columns[3:].tolist()]

	
	return pivot.to_dict('records')

	


