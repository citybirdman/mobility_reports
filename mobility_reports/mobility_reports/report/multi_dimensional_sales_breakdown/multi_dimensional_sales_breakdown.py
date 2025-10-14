# Copyright (c) 2025, Aerele Technologies Private Limited and contributors
# For license information, please see license.txt

import frappe
import pandas as pd # type: ignore
from datetime import datetime
now=datetime.now()



def execute(filters=None):
	data=get_data(filters)
	columns=get_columns(data,filters)
	return columns, data


def get_columns(data, filters):
	months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
	          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
	
	columns = []

	# Determine which "dimension" columns to include
	if filters.get('metric') == 'Item (Qty)':
		# Show both item_name and category
		for col in ['item_name', 'catogory']:
			if col in data[0]:
				columns.append({
					"fieldname": col,
					"label": col.replace('_', ' ').title(),
					"fieldtype": "Data",
					"width": 150
				})
	else:
		# Show only the first grouping dimension (e.g., sales channel, etc.)
		first_key = list(data[0].keys())[0]
		if first_key not in months:
			columns.append({
				"fieldname": first_key,
				"label": first_key.replace('_', ' ').title(),
				"fieldtype": "Data",
				"width": 150
			})

	# Add month columns
	for month in months:
		columns.append({
			"fieldname": month,
			"label": month,
			"fieldtype": "Float",
			"width": 150
		})

	# Add any additional columns like YTD, variance, var %
	for col in data[0].keys():
		if col not in months and col not in ['item_name', 'catogory', 'custom_location_zone', 'custom_Sales_channel', 'Country']:
			columns.append({
				"fieldname": col,
				"label": col.replace('_', ' ').title(),
				"fieldtype": "Data",
				"width": 150
			})

	return columns



def get_data(filters):

	omg_24=pd.read_csv("https://www.dropbox.com/scl/fi/yvz5bm17177jdu0ok4z3v/OMG-Sales-24-2024-Sales.csv?rlkey=fe05jv6pp86h4ma6aehe6w8os&st=x1cgkd35&dl=1")

	if filters.get('metric') == 'Item (Amount)':
		data=frappe.db.sql(    """ 
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
		""", as_dict=True)
		omg_25=pd.DataFrame([dict(row) for row in data])
	else:
		data=frappe.db.sql(    """ 
		SELECT 
			sii.name ,
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

				""", as_dict=True)
		omg_25=pd.DataFrame([dict(row) for row in data])



	df=pd.concat([omg_24, omg_25]).reset_index(drop=True)
	df['posting_date'] = pd.to_datetime(df['posting_date'], format='mixed', dayfirst=True).dt.strftime('%Y-%m-%d')

	df.territory=df.territory.str.strip().str.upper()
	df.custom_location_zone=df.custom_location_zone.str.strip().str.upper()
	df.custom_Sales_channel=df.custom_Sales_channel.str.strip().str.title()
	df.custom_invoice_number=df.custom_invoice_number.str.strip().str.title()
	df.custom_status=df.custom_status.str.strip().str.title()
	df.custom_order_method=df.custom_order_method.str.strip().str.title()
	df.customer_name=df.customer_name.str.strip().str.title()
	df.item_code=df.item_code.str.strip().str.title()
	df.item_name=df.item_name.str.strip().str.title()
	df.custom_payment_status=df.custom_payment_status.str.strip().str.title()
	df['catogory']=df.item_code.str.split('-').str[0].str.strip().str.title()
	df['item_name'] = df.item_code.str.split('-').str[1].str.strip().str.title()

	months_list=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

	date_list = pd.DataFrame({'date':pd.date_range(start=f'2024-01-01', end=f'{now.year}-12-31',)})

	date_list['date']=pd.to_datetime(date_list['date'])

	df['posting_date'] = pd.to_datetime(df['posting_date'])

	date_list['month'] = date_list['date'].dt.month_name().str[:3]

	merged_df=df.merge(date_list, how='right', right_on='date', left_on='posting_date')

	def calculate_ytd(df, date_col, group_cols, value_col):
		df = df.copy()
		df[date_col] = pd.to_datetime(df[date_col])
		df['year'] = df[date_col].dt.year
		df['month'] = df[date_col].dt.to_period('M')
		
		# Aggregate by group + year + month
		grouped = df.groupby(group_cols + ['year'], as_index=False)[value_col].sum()
		grouped = grouped.sort_values(group_cols + ['year'])
		
		# Calculate YTD cumulative sum
		ytd_col = f'YTD_{value_col}'
		grouped[ytd_col] = grouped.groupby(group_cols + ['year'])[value_col].cumsum()
		
		return grouped
	

	def sales(df,month,by,how):
		df['month'] = pd.Categorical(df['month'], categories=months_list, ordered=True)

		df = df.sort_values('month')

		this_year =df[df.date>=f'{(now.year)}-01-01'].reset_index(drop=True)
		# Create pivot table
		if how == 'item_name':
			pivot_df = this_year.pivot_table(
				columns='month',
				index=[f'{how}','catogory'],
				values=f'{by}',
				aggfunc='sum',
				fill_value=0,
				observed=True
			).reset_index()
			desired_columns = [f'{how}','catogory'] + month

		else:
			pivot_df = this_year.pivot_table(
				columns='month',
				index=f'{how}',
				values=f'{by}',
				aggfunc='sum',
				fill_value=0,
				observed=True
			).reset_index()
			desired_columns = [f'{how}'] + month

		for month in month:
			if month not in pivot_df.columns:
				pivot_df[month] = 0

		pivot_df = pivot_df[desired_columns]
		total_row = pivot_df.select_dtypes(include='number').sum()
		total_row[f'{how}'] = 'Total'
		pivot_df = pd.concat([pivot_df, pd.DataFrame([total_row])], ignore_index=True)

		
		df['year']=df['date'].dt.year
		df[f'{by}']=df[f'{by}'].fillna(0)
		df.sort_values(by=['date'], ascending=True, inplace=True)
		if how == 'item_name':
			df_variance=calculate_ytd(df, 'date', [f'{how}','catogory'], f'{by}').pivot_table(
				columns='year',
				index=[f'{how}', 'catogory'],
				values=f'YTD_{by}',
				fill_value=0,
				observed=True).reset_index()
		else:
			df_variance=calculate_ytd(df, 'date', [f'{how}'], f'{by}').pivot_table(
			columns='year',
			index=f'{how}',
			values=f'YTD_{by}',
			fill_value=0,
			observed=True).reset_index() 

		df_variance = df_variance.rename(columns={
		df_variance.columns[-1]: f'YTD ({df_variance.columns[-1]})',
		df_variance.columns[-2]: f'YTD ({df_variance.columns[-2]})'
	})
		latest_year_col = df_variance.columns[-1]
		previous_year_col = df_variance.columns[-2]

		df_variance['variance'] = df_variance[latest_year_col] - df_variance[previous_year_col]
		df_variance['var %'] = (
			(df_variance['variance'] / df_variance[previous_year_col].replace(0, pd.NA)) * 100
		).round(2)

		df_variance['var %'] = df_variance['var %'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "")
		df_variance.drop(columns=['variance'], inplace=True)
		if how == 'item_name':
			df=pivot_df.merge(df_variance, how='left', on=[f'{how}', 'catogory'])
		else:
			df=pivot_df.merge(df_variance, how='left', on=f'{how}')
		return df

	if filters.get('metric') == 'Item (Qty)':
		pivot_data=sales(merged_df,months_list,'qty','item_name')
	elif filters.get('metric') == 'Item (Amount)':
		pivot_data=sales(merged_df,months_list,'base_amount','item_name')
	elif filters.get('metric') == 'Location Zone (Qty)':
		pivot_data=sales(merged_df,months_list,'qty','custom_location_zone')
	elif filters.get('metric') == 'Sales Channel (Qty)':
		pivot_data=sales(merged_df,months_list,'qty','custom_Sales_channel')
	else:
		pivot_data=sales(merged_df,months_list,'qty','territory')
		pivot_data.rename(columns={'territory': 'Country'}, inplace=True)
	
	
	pivot_data.fillna(0, inplace=True)

	return pivot_data.to_dict(orient='records')