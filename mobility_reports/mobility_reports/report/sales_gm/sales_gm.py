# Copyright (c) 2025, Aerele Technologies Private Limited and contributors
# For license information, please see license.txt

import frappe
import pandas as pd # type: ignore
from datetime import datetime

now=datetime.now()




def execute(filters=None):
	data=get_data()
	columns = get_columns(data)
	return columns, data

def get_columns(data):
	months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
	          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
	
	columns = []

	for col in ['item_name', 'catogory']:
				columns.append({
					"fieldname": col,
					"label": col.replace('_', ' ').title(),
					"fieldtype": "Data",
					"width": 150
				})
	
	for month in months:
		columns.append({
			"fieldname": month,
			"label": month,
			"fieldtype": "Float",
			"width": 150
		})

	for col in data[0].keys():
		if col not in months and col not in ['item_name', 'catogory']:
			columns.append({
				"fieldname": f"{col}",
				"label": col.replace('_', ' ').title(),
				"fieldtype": "Float",
				"width": 150
			})

	return columns 
def get_data():

	cogs=frappe.db.sql("""
		SELECT
			i.name AS item_name,
			si.posting_date,
			(sii.base_net_amount - ABS(ifnull(sle_agg.cogs, 0))) AS gross_margin,
			ROUND(((sii.base_net_amount - ABS(ifnull(sle_agg.cogs, 0))) / (sii.qty * sii.base_rate)) * 100, 2) AS gross_margin_percent
		FROM
			`tabSales Invoice Item` sii
		JOIN
			`tabSales Invoice` si ON si.name = sii.parent AND si.docstatus = 1
		JOIN tabItem i ON i.name = sii.item_code
		LEFT JOIN (
			SELECT
				voucher_detail_no,
				SUM(stock_value_difference) AS cogs
			FROM
				`tabStock Ledger Entry`
			WHERE
				voucher_type = 'Sales Invoice'
				and is_cancelled = 0
			GROUP BY
				voucher_detail_no
		) sle_agg ON sle_agg.voucher_detail_no = sii.name
		WHERE
			si.docstatus = 1
			AND si.status <> 'Cancelled'
			
		""",as_dict=True,)
	cogs= pd.DataFrame([dict(row) for row in cogs])
	date_list = pd.DataFrame({'date':pd.date_range(start=f'2025-01-01', end=f'{now.year}-12-31',)})

	cogs['catogory']=cogs.item_name.str.split('-').str[0].str.strip().str.title()
	cogs['item_name']= cogs.item_name.str.split('-').str[1].str.strip().str.title()

	date_list['date']=pd.to_datetime(date_list['date'])

	cogs['posting_date'] = pd.to_datetime(cogs['posting_date'])

	date_list['month'] = date_list['date'].dt.month_name().str[:3]
	cogs=cogs.merge(date_list, how='right', right_on='date', left_on='posting_date')


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
	


	months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
	def cogs_fun(cogs,months):
		cogs['month'] = pd.Categorical(cogs['month'], categories=months, ordered=True)

		cogs = cogs.sort_values('month')
		
		this_year =cogs[cogs.date>=f'{(now.year)}-01-01'].reset_index(drop=True)
		# Create pivot table
		pivot_df = this_year.pivot_table(
			columns='month',
			index=['item_name','catogory'],
			values=f'gross_margin',
			aggfunc='sum',
			fill_value=0,
			observed=True
		).reset_index()
		desired_columns = ['item_name','catogory'] + months
		for month in months:
			if month not in pivot_df.columns:
				pivot_df[month] = 0

		pivot_df = pivot_df[desired_columns]
		total_row = pivot_df.select_dtypes(include='number').sum()
		total_row['item_name'] = 'Total'
		pivot_df = pd.concat([pivot_df, pd.DataFrame([total_row])], ignore_index=True)
		df_ytd=calculate_ytd(cogs, 'date', ['item_name','catogory'], f'gross_margin').pivot_table(
			columns='year',
			index=['item_name', 'catogory'],
			values=f'YTD_gross_margin',
			fill_value=0,
			observed=True).reset_index() 
		df_ytd.rename(columns={
			df_ytd.columns[-1]: f'YTD ({df_ytd.columns[-1]})',
		}, inplace=True)
		return pivot_df.merge(df_ytd, how='left', on=['item_name', 'catogory'])

	cogs_df=cogs_fun(cogs,months)
	cogs_df = cogs_df.fillna(0).round(2)
	return cogs_df.to_dict(orient='records')