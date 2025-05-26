# Copyright (c) 2025, Aerele Technologies Private Limited and contributors
# For license information, please see license.txt

import frappe
import pandas as pd  # type: ignore
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine # type: ignore
import requests 
now=datetime.now()
site = frappe.local.site


def execute(filters=None):
	data = get_data()
	columns= get_columns(data)
	return columns, data

def get_columns(data):
	columns=[]
	for col in data[0].keys():
		columns.append({
			"label": col.replace('_', ' ').title(),
			"fieldname": col,
			"fieldtype": "Data" if col not in ['amount(2024)', 'qty(2024)', 'invoice_no_2024', 'invoice_no_2025'] else "Currency" if 'amount' in col else "Float",
			"width": 170 if col in ['customer_name', 'Sales_channel'] else 150
		})
	return columns

def get_data():


	omg_24=pd.read_csv("https://www.dropbox.com/scl/fi/yvz5bm17177jdu0ok4z3v/OMG-Sales-24-2024-Sales.csv?rlkey=fe05jv6pp86h4ma6aehe6w8os&st=x1cgkd35&dl=1")


	queries=[""" 	
		SELECT 
			sii.name,
			si.name AS sales_invoice_id,
			si.customer,
			c.customer_name,
			c.custom_location_zone,
			c.territory,
			c.custom_source,
			sii.warehouse,
			i.name as item_code,
			i.item_name,
			si.posting_date,
			si.custom_invoice_number,
			si.custom_shipping_cost,
			c.custom_status,
			si.custom_order_method,
			si.custom_sales_chanal AS custom_Sales_channel,
			si.custom_payment_status,
			MAX(sii.conversion_factor) AS conversion_factor,
			MAX(si.custom_code_amount) AS base_discount_amount,
			MAX(si.custom_additional_discount_amount1) AS discount_amount,
			SUM(sii.qty) AS qty,
			MAX(sii.base_rate) AS base_rate,
			SUM(sii.base_net_amount) AS base_amount,
			(MAX(si.custom_vat) +max( si.custom_shipping_cost))+ SUM(sii.base_amount) AS amount_after_vat
		FROM `tabSales Invoice` si
		JOIN `tabSales Invoice Item` sii 
			ON si.name = sii.parent 
		join `tabCustomer` c 
			on si.customer = c.name
		join tabItem i
			on i.name = sii.item_code
		WHERE si.docstatus = 1 
		AND si.status <> 'Cancelled'
		and si.docstatus = 1
		GROUP BY 
			sii.name,
			si.name,
			si.customer,
			si.customer_name,
			si.custom_location_zone,
			si.territory,
			sii.warehouse,
			sii.item_code,
			sii.item_name,
			si.posting_date,
			si.custom_invoice_number,
			si.custom_shipping_cost,
			c.custom_status,
			si.custom_order_method,
			si.custom_sales_chanal,
			si.custom_payment_status
		""",

		""" 
		SELECT 
			sii.name AS sales_invoice_item_id,
			si.name AS sales_invoice_id,
			si.customer,
			c.customer_name,
			c.custom_location_zone,
			c.territory,
			c.custom_source,
			sii.warehouse,
			i.name AS item_code,
			i.item_name,
			si.posting_date,
			si.custom_invoice_number,
			si.custom_shipping_cost,
			c.custom_status,
			si.custom_order_method,
			si.custom_sales_chanal AS custom_Sales_channel,
			si.custom_payment_status,
			
			MAX(sii.conversion_factor) AS conversion_factor,
			MAX(si.custom_code_amount) AS base_discount_amount,
			MAX(si.custom_additional_discount_amount1) AS discount_amount,
			SUM(ABS(sle.actual_qty)) AS qty,
			MAX(sii.base_rate) AS base_rate,
			SUM(sii.base_net_amount) AS base_amount,
			
			-- Total = base_amount + shipping + VAT
			(MAX(si.custom_vat) + MAX(si.custom_shipping_cost) + SUM(sii.base_net_amount)) AS amount_after_vat

		FROM 
			`tabSales Invoice` si
		JOIN 
			`tabSales Invoice Item` sii ON si.name = sii.parent 
		JOIN 
			`tabCustomer` c ON si.customer = c.name
		JOIN 
			`tabItem` i ON i.name = sii.item_code
		JOIN 
			`tabStock Ledger Entry` sle ON sle.voucher_detail_no = sii.name

		WHERE 
			si.docstatus = 1
			AND si.status != 'Cancelled'
			AND sle.is_cancelled = 0
			AND sle.voucher_type = 'Sales Invoice'

		GROUP BY 
			sii.name,
			si.name,
			si.customer,
			c.customer_name,
			c.custom_location_zone,
			c.territory,
			c.custom_source,
			sii.warehouse,
			i.name,
			i.item_name,
			si.posting_date,
			si.custom_invoice_number,
			si.custom_shipping_cost,
			c.custom_status,
			si.custom_order_method,
			si.custom_sales_chanal,
			si.custom_payment_status

				"""
	]


	def run_query(sql):
		frappe.init(site=site)
		frappe.connect()  # Create a new connection
		try:
			result = frappe.db.sql(sql, as_dict=True)
			return result
		finally:
			frappe.destroy() 


	with ThreadPoolExecutor(max_workers=2) as executor:
		futures = [executor.submit(run_query, q) for q in queries]
		results = [f.result() for f in futures]

	sales_amount=pd.DataFrame([dict(row) for row in results[0]])
	sales_qty=pd.DataFrame([dict(row) for row in results[1]])

	sales_amount_concated= pd.concat([sales_amount, omg_24], ignore_index=True)
	sales_amount_concated.territory=sales_amount_concated.territory.str.strip().str.upper()
	sales_amount_concated.custom_location_zone=sales_amount_concated.custom_location_zone.str.strip().str.upper()
	sales_amount_concated.custom_Sales_channel=sales_amount_concated.custom_Sales_channel.str.strip().str.title()
	sales_amount_concated.custom_invoice_number=sales_amount_concated.custom_invoice_number.str.strip().str.title()
	sales_amount_concated.custom_status=sales_amount_concated.custom_status.str.strip().str.title()
	sales_amount_concated.custom_order_method=sales_amount_concated.custom_order_method.str.strip().str.title()
	sales_amount_concated.customer_name=sales_amount_concated.customer_name.str.strip().str.title()
	sales_amount_concated.item_code=sales_amount_concated.item_code.str.strip().str.title()
	sales_amount_concated.item_name=sales_amount_concated.item_name.str.strip().str.title()
	sales_amount_concated.custom_payment_status=sales_amount_concated.custom_payment_status.str.strip().str.title()
	sales_amount_concated['posting_date'] = pd.to_datetime(sales_amount_concated['posting_date'])


	customer_df = sales_amount_concated[['customer_name', 'base_amount']].groupby('customer_name', dropna=True).sum().reset_index().sort_values(by='base_amount', ascending=False)
	total_amount = customer_df['base_amount'].sum()
	customer_df['runing_total']=customer_df['base_amount'].cumsum()
	customer_df['percent_of_total'] = customer_df['runing_total'].apply(lambda x: (x / total_amount * 100))
	top_80=customer_df[customer_df.percent_of_total<=80.00].reset_index(drop=True)
	omg_24=sales_amount_concated[sales_amount_concated.posting_date.dt.year==2024].reset_index(drop=True)
	customer_24=omg_24[omg_24.customer_name.isin(top_80.customer_name)].reset_index(drop=True)
	customer_24['year']=customer_24['posting_date'].dt.year


	customer_25_amount=sales_amount_concated[sales_amount_concated.posting_date.dt.year==2025].reset_index(drop=True)

	customer_25_amount=customer_25_amount[customer_25_amount.customer_name.isin(top_80.customer_name)].reset_index(drop=True)
	customer_25_amount['year']=customer_25_amount['posting_date'].dt.year
	customer_25_amount['qur']=customer_25_amount['posting_date'].dt.to_period('Q')

	customer_25_qty=sales_qty[sales_qty.customer_name.isin(top_80.customer_name)].reset_index(drop=True)
	
	customer_25_qty.territory=customer_25_qty.territory.str.strip().str.upper()
	customer_25_qty.custom_location_zone=customer_25_qty.custom_location_zone.str.strip().str.upper()
	customer_25_qty.custom_Sales_channel=customer_25_qty.custom_Sales_channel.str.strip().str.title()
	customer_25_qty.custom_invoice_number=customer_25_qty.custom_invoice_number.str.strip().str.title()
	customer_25_qty.custom_status=customer_25_qty.custom_status.str.strip().str.title()
	customer_25_qty.custom_order_method=customer_25_qty.custom_order_method.str.strip().str.title()
	customer_25_qty.customer_name=customer_25_qty.customer_name.str.strip().str.title()
	customer_25_qty.item_code=customer_25_qty.item_code.str.strip().str.title()
	customer_25_qty.item_name=customer_25_qty.item_name.str.strip().str.title()
	customer_25_qty.custom_payment_status=customer_25_qty.custom_payment_status.str.strip().str.title()
	customer_25_qty['posting_date'] = pd.to_datetime(customer_25_qty['posting_date'])
	
	customer_25_qty['year']=customer_25_qty['posting_date'].dt.year
	customer_25_qty['qur']=customer_25_qty['posting_date'].dt.to_period('Q')


	invoice_no_24 = customer_24[['customer_name','custom_Sales_channel', 'custom_invoice_number','year']].drop_duplicates().groupby(['customer_name','custom_Sales_channel', 'year'], dropna=True).count().reset_index()
	invoice_no_25 = customer_25_amount[['customer_name', 'custom_Sales_channel','custom_invoice_number','year']].drop_duplicates().groupby(['customer_name','custom_Sales_channel', 'year'], dropna=True).count().reset_index()

	customer_amount_24=customer_24.pivot_table(
    columns='year',
    index=['customer_name', 'custom_Sales_channel'],
    values='base_amount',
    aggfunc='sum',
    fill_value=0,
    observed=True
	).reset_index()
	customer_amount_24.columns = [
		f'amount({col})' if col not in ['customer_name', 'custom_Sales_channel'] else col
		for col in customer_amount_24.columns
	]
	for col in customer_amount_24.columns[2:]:
		
		total_amount_24=customer_amount_24[col].sum()
		customer_amount_24[f'{col} %']=customer_amount_24[col].apply(lambda x: f"{(x / total_amount_24 * 100).round(0):.2f} %")


	customer_qty_24=customer_24.pivot_table(
    columns='year',
    index=['customer_name', 'custom_Sales_channel'],
    values='qty',
    aggfunc='sum',
    fill_value=0,
    observed=True
	).reset_index()
	customer_qty_24.columns = [
		f'qty({col})' if col not in ['customer_name', 'custom_Sales_channel'] else col
		for col in customer_qty_24.columns
	]
	for col in customer_qty_24.columns[2:]:
		
		total_qty_24=customer_qty_24[col].sum()
		customer_qty_24[f'{col} %']=customer_qty_24[col].apply(lambda x: f"{(x / total_qty_24 * 100).round(0):.2f} %")



	df_24=customer_amount_24.merge(customer_qty_24, how='outer', on=['customer_name', 'custom_Sales_channel']).merge(invoice_no_24,how='left', on=['customer_name','custom_Sales_channel']).fillna(0).reset_index(drop=True).drop(columns=['year']).rename(columns={'custom_invoice_number':'invoice_no_2024'})


	customer_amount_25=customer_25_amount.pivot_table(
    columns='qur',
    index=['customer_name', 'custom_Sales_channel'],
    values='base_amount',
    aggfunc='sum',
    fill_value=0,
    observed=True
	).reset_index()
	customer_amount_25.columns = [
		f'amount({col})' if col not in ['customer_name', 'custom_Sales_channel'] else col
		for col in customer_amount_25.columns
	]
	for col in customer_amount_25.columns[2:]:
		total_amount_25=customer_amount_25[f'{col}'].sum()
		customer_amount_25[f'{col} %']=customer_amount_25[col].apply(lambda x: f"{(x / total_amount_25 * 100).round(0):.2f} %")


	customer_qty_25=customer_25_qty.pivot_table(
    columns='qur',
    index=['customer_name', 'custom_Sales_channel'],
    values='qty',
    aggfunc='sum',
    fill_value=0,
    observed=True
	).reset_index()
	customer_qty_25.columns = [
		f'qty({col})' if col not in ['customer_name', 'custom_Sales_channel'] else col
		for col in customer_qty_25.columns
	]
	for col in customer_qty_25.columns[2:]:
		total_qty_25=customer_qty_25[f'{col}'].sum()
		customer_qty_25[f'{col} %']=customer_qty_25[col].apply(lambda x: f"{(x / total_qty_25 * 100).round(0):.2f} %")

	df_25=customer_amount_25.merge(customer_qty_25, how='outer', on=['customer_name', 'custom_Sales_channel']).merge(invoice_no_25,how='left', on=['customer_name', 'custom_Sales_channel']).fillna(0).reset_index(drop=True).drop(columns=['year']).rename(columns={'custom_invoice_number':'invoice_no_2025'})
	print(df_24.head())
	print(df_25.head())
	df=df_24.merge(df_25, how='outer', on=['customer_name', 'custom_Sales_channel']).fillna(0).reset_index(drop=True).merge(sales_amount_concated[['customer_name','custom_location_zone',]], how='left', on='customer_name').drop_duplicates().reset_index(drop=True)
	first_columns = [
		'customer_name', 'custom_Sales_channel',
		'amount(2024)', 'amount(2024) %',
		'qty(2024)', 'qty(2024) %',
		'invoice_no_2024',
	]

	# Ensure quarters are ordered properly
	quarters = sorted(set(customer_25_qty['qur']))
	quarter_cols = []
	for q in quarters:
		quarter_cols.extend([
			f'amount({q})', f'amount({q}) %',
			f'qty({q})', f'qty({q}) %'
		])

	# Last columns
	last_columns = ['invoice_no_2025', 'custom_location_zone']

	# Reorder DataFrame
	df = df[first_columns + quarter_cols + last_columns]
	df.rename(columns={
		'custom_Sales_channel': 'sales_channel',
		'custom_location_zone': 'location_zone',}, inplace=True)
	return df.to_dict(orient='records')





