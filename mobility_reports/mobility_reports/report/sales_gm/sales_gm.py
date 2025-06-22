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
	ssl_url = "https://www.dropbox.com/scl/fi/1hj515q7rykj0l2urpytn/omg.pem?rlkey=3brhxb9x52v23myeegt85983a&st=31ostnmu&dl=1"
	response = requests.get(ssl_url)

	cert_path = "n1-ksa.frappe.cloud.omg.pem"
	with open(cert_path, "wb") as f:
		f.write(response.content)

	ssl_args = {"ssl": {"ca": cert_path}}
	connection_string = f"mysql+pymysql://174a179b828f397:f0f036846ffcf44c3def@n1-ksa.frappe.cloud:3306/_99a43d5c723190d4"
	engine = create_engine(connection_string, connect_args=ssl_args)
	cogs=pd.read_sql("""
				WITH sales_amount AS (
			select c.custom_location_zone as location_zone,
				s.posting_date,
				sii.item_code,
				sum(sii.base_net_amount)+IFNULL(sum(t.discount_amount),0)as amount
				from   `tabSales Invoice` s
			join `tabSales Invoice Item` sii on s.name=sii.parent
			join `tabCustomer` c on s.customer=c.name
			left join(
				SELECT
			t.parent,
			JSON_UNQUOTE(j.VALUE) AS item_name ,-- Extract the item name
			CAST(JSON_EXTRACT(t.item_wise_tax_detail, CONCAT('$."', JSON_UNQUOTE(j.VALUE), '"[1]')) AS DECIMAL(18,8)) AS discount_amount
			FROM
			`tabSales Taxes and Charges` t
			JOIN
			JSON_TABLE(
				JSON_KEYS(t.item_wise_tax_detail),
				'$[*]' COLUMNS (
					VALUE VARCHAR(255) PATH '$'
				)
			) AS j
			WHERE 
			t.docstatus = 1 
			AND t.account_head = 'Discounts - OMG'
			AND t.item_wise_tax_detail IS NOT NULL


			) t on s.name=t.parent  and sii.item_code=t.item_name
			where  s.docstatus=1  
			GROUP BY s.posting_date,sii.item_code
			order by amount desc
			),
			cogs AS (
			SELECT 
				item_code,
				posting_date ,
				SUM(stock_value_difference) * -1 AS c
			FROM 
				`tabStock Ledger Entry`
			WHERE 
				is_cancelled = 0
				AND voucher_type = 'Sales Invoice'
			GROUP BY 
				item_code, DATE(posting_date)
			)
			SELECT 
			item_code,
			posting_date,
			location_zone,
			SUM(amount) AS base_net_amount,
			SUM(c) AS total_cogs,
			SUM(amount) - SUM(c) AS gross_profit,
			SUM(c) / SUM(amount) * 100 AS gross_margin
			FROM (
			SELECT 
				sa.item_code,
				sa.location_zone,
				sa.posting_date,
				sa.amount,
				co.c
			FROM sales_amount sa
			LEFT JOIN cogs co 
				ON sa.item_code = co.item_code 
				AND sa.posting_date = co.posting_date

			UNION

			SELECT 
				co.item_code,
				sa.location_zone,
				co.posting_date,
				sa.amount,
				co.c
			FROM cogs co
			LEFT JOIN sales_amount sa 
				ON sa.item_code = co.item_code 
				AND sa.posting_date = co.posting_date
			) AS result
			GROUP BY item_code,
			posting_date

			
		""",engine)
	# cogs= pd.DataFrame([dict(row) for row in cogs])
	date_list = pd.DataFrame({'date':pd.date_range(start=f'2025-01-01', end=f'{now.year}-12-31',)})

	cogs['catogory']=cogs.item_code.str.split('-').str[0].str.strip().str.title()
	cogs['item_name']= cogs.item_code.str.split('-').str[1].str.strip().str.title()

	date_list['date']=pd.to_datetime(date_list['date'])

	cogs['posting_date'] = pd.to_datetime(cogs['posting_date'])

	date_list['month'] = date_list['date'].dt.month_name().str[:3]
	cogs=cogs.merge(date_list, how='right', right_on='date', left_on='posting_date')

	print('ahmed')
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
	print(cogs_df.head())
	print(cogs.head())

	cogs_df = cogs_df.fillna(0).round(2)
	return cogs_df.to_dict(orient='records')	