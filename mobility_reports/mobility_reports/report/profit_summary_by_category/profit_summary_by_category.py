	# Copyright (c) 2025, Aerele Technologies Private Limited and contributors
	# For license information, please see license.txt

import frappe
import pandas as pd # type: ignore
from datetime import datetime
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine # type: ignore
import requests 
now=datetime.now()
import numpy as np


def execute(filters=None):
	data=get_data(filters)
	columns = get_columns(data)
	return columns, data


def get_columns(data):
		if not data:
			return []

		# Extract all fieldnames from the first data row
		first_row = data[0]
		columns = []

		for key in first_row:
			columns.append({
				"label": key.replace("_", " ").title(),  # auto-label
				"fieldname": key,
				"fieldtype": "Currency" if key not in ("catogory", "MOM",'location_zone','item_name') else (
					"Data" if key in ("catogory",'location_zone','item_name') else "Percent"
				),
				"width": 120 if key not in ("catogory", "MOM",'location_zone','item_name') else 180 if key in ("catogory",'location_zone','item_name') else 100
			})

		return columns

def get_data(filters):
    
		catogory=filters.get("category")
		quarterly=filters.get("quarterly")
		print(catogory)
		ssl_url = "https://www.dropbox.com/scl/fi/1hj515q7rykj0l2urpytn/omg.pem?rlkey=3brhxb9x52v23myeegt85983a&st=31ostnmu&dl=1"
		response = requests.get(ssl_url)

		cert_path = "n1-ksa.frappe.cloud.omg.pem"
		with open(cert_path, "wb") as f:
			f.write(response.content)

		ssl_args = {"ssl": {"ca": cert_path}}
		connection_string = f"mysql+pymysql://174a179b828f397:f0f036846ffcf44c3def@n1-ksa.frappe.cloud:3306/_99a43d5c723190d4"
		engine = create_engine(connection_string, connect_args=ssl_args)
		def get_quarter_date_range(q: str, year: int):
			qmap = {
				"Q1": (1, 3),
				"Q2": (4, 6),
				"Q3": (7, 9),
				"Q4": (10, 12),
			}
			start_month, end_month = qmap[q]
			from_date = datetime(year, start_month, 1)
			to_date = datetime(year, end_month, 1) + pd.offsets.MonthEnd(0)
			x=pd.date_range(start= from_date.strftime('%Y-%m-%d'),end= to_date.strftime('%Y-%m-%d'))
			return x

		x=get_quarter_date_range(quarterly, now.year)

		print(x[0],x[-1])
		queries=[
			f"""WITH sales_amount AS (
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
			""",
			""" SELECT
					date_format(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH),'%%M') AS month,
					SUM(gl.credit) - SUM(gl.debit) AS value
				FROM `tabGL Entry` gl
				JOIN `tabAccount` a ON a.name = gl.account
				WHERE gl.is_cancelled = 0
				AND a.root_type IN ('Income', 'Expense')
      			AND gl.posting_date between DATE_SUB(date_format(CURRENT_DATE, '%%Y-%%m-01'), INTERVAL 1 MONTH) and last_day(DATE_SUB(date_format(CURRENT_DATE, '%%Y-%%m-30'), INTERVAL 1 MONTH))
			""",

			"""
				SELECT
					DATE_FORMAT(CURRENT_DATE(), '%%M') AS month,
					SUM(gl.credit) - SUM(gl.debit) AS value
				FROM `tabGL Entry` gl
				JOIN `tabAccount` a ON a.name = gl.account
				WHERE gl.is_cancelled = 0
				AND a.root_type IN ('Income', 'Expense')
				AND gl.posting_date between DATE_FORMAT(CURRENT_DATE(), '%%Y-%%m-01') and LAST_DAY(DATE_FORMAT(CURRENT_DATE(), '%%Y-%%m-01'))
			""",


			f"""select sum(gl.credit)-sum(gl.debit)as value  from `tabGL Entry` gl
			join tabAccount a on a.name=gl.account
			where gl.is_cancelled=0 and root_type in('Income','Expense') 
			and gl.posting_date between '{x[0]}' and '{x[-1]}'
			"""
		]

		def run_query(sql):
			return pd.read_sql(sql,engine)

		with ThreadPoolExecutor(max_workers=5) as executor:
			futures = [executor.submit(run_query, q) for q in queries]
   
		if catogory=='Summary':
			gp_df=futures[0].result()
			net_profit_lostmonth=futures[1].result()
			net_profit_thismonth=futures[2].result()
			net_profit=futures[3].result()

			gp_df['catogory']=gp_df.item_code.str.split('-').str[0].str.strip().str.title()
			gp_df['item_name'] = gp_df.item_code.str.split('-').str[1].str.strip().str.title()
			two_month=gp_df


			gp_df.posting_date=pd.to_datetime(gp_df.posting_date)

			# %%
			two_month.posting_date=two_month.posting_date.sort_values(ascending=False)


			# %%
			two_month['date']=two_month.posting_date.dt.month_name()

			# %%
			two_month=two_month[two_month.posting_date>=(now-timedelta(days=31)).strftime("%Y-%m-01")]

			two_month_gp=two_month.groupby(['catogory','date'])[['gross_profit','base_net_amount']].sum().sort_values('gross_profit',ascending=False).reset_index()
			two_month_gp['GM']=((two_month_gp['gross_profit']/two_month_gp['base_net_amount'])*100).round(2)

			# %%
			two_month_gp.rename(columns={'gross_profit':'value'},inplace=True)

			# %%
			total_gross_profit=pd.DataFrame({'value':[two_month_gp['value'].sum()]})
			total_gross_profit['catogory']='Total Gross Profit'
			total_gross_profit['date']=two_month_gp['date'].unique()[0]
			total_gross_margin=pd.DataFrame({'value':[((two_month_gp['value'].sum()/two_month_gp['base_net_amount'].sum())*100).round(2)]})
			total_gross_margin['catogory']='Total Gross Margin'
			total_gross_margin['date']=two_month_gp['date'].unique()[0]
			net_margin=pd.DataFrame({'value':[(net_profit_lostmonth['value'].sum()/two_month_gp['base_net_amount'].sum())*100]})
			net_margin['catogory']='Net Margin'
			net_margin['date']=two_month_gp['date'].unique()[0]

			# %%
			net_profit_lostmonth['catogory']='Net Profit'
			net_profit_lostmonth['date']=two_month_gp['date'].unique()[0]

			last_month=pd.concat([two_month_gp,total_gross_profit,total_gross_margin,net_profit_lostmonth,net_margin]).reset_index(drop=True)


			# %%
			last_month=last_month.pivot_table(index=['catogory'],columns='date',values='value',aggfunc='sum',fill_value=0,observed=True,sort=False).reset_index()

			# %%
			gp_df=gp_df[gp_df.posting_date.isin(x)]
			gp=gp_df.groupby(['catogory'])[['gross_profit','base_net_amount']].sum().sort_values('gross_profit',ascending=False).reset_index()
			gp['GM']=((gp['gross_profit']/gp['base_net_amount'])*100).round(2)
			# %%
			total_gross_profit=pd.DataFrame({'value':[gp['gross_profit'].sum()]})
			total_gross_profit['catogory']='Total Gross Profit'
			total_gross_margin=pd.DataFrame({'value':[((gp['gross_profit'].sum()/gp['base_net_amount'].sum())*100).round(2)]})
			total_gross_margin['catogory']='Total Gross Margin'
			net_margin=pd.DataFrame({'value':[(net_profit['value'].sum()/gp['base_net_amount'].sum())*100]})
			net_margin['catogory']='Net Margin'
			net_margin['value']=net_margin['value'].apply(lambda x: f'{x:.2f}%')


			# %%
			net_profit['catogory']='Net Profit'
			net_profit['value']=net_profit['value'].apply(lambda x: f'{x:.2f}')	

			# %%
			gp.rename(columns={'gross_profit':'value'},inplace=True)

			# %%
			df=pd.concat([gp,total_gross_profit,total_gross_margin,net_profit,net_margin]).reset_index(drop=True)

			# %%
			q_df=df.pivot_table(index='catogory',values='value',aggfunc='sum',fill_value=0,observed=True,sort=False).reset_index()

			q_df.rename(columns={'value':filters.get('quarterly')},inplace=True)

			# %%
			lostToMonths=(pd.date_range(start=now-timedelta(days=31),end=now)).month_name().unique()

			
			# %%
			current_month=two_month[two_month.date.isin([lostToMonths[-1]])]

			# %%
			current_month=current_month.groupby(['catogory','date'])[['gross_profit','base_net_amount']].sum().sort_values('gross_profit',ascending=False).reset_index()
			current_month['GM']=((current_month['gross_profit']/current_month['base_net_amount'])*100).round(2)

			# %%
			current_month.rename(columns={'gross_profit':'value'},inplace=True)

			# %%
			total_gross_profit=pd.DataFrame({'value':[current_month['value'].sum()]})
			total_gross_profit['catogory']='Total Gross Profit'
			total_gross_profit['date']=lostToMonths[-1]
			total_gross_margin=pd.DataFrame({'value':[((current_month['value'].sum()/current_month['base_net_amount'].sum())*100).round(2)]})
			total_gross_margin['catogory']='Total Gross Margin'
			total_gross_margin['date']=lostToMonths[-1]
			net_margin=pd.DataFrame({'value':[(net_profit_thismonth['value'].sum()/current_month['base_net_amount'].sum())*100]})
			net_margin['catogory']='Net Margin'
			net_margin['date']=lostToMonths[-1]
			# net_margin['value']=net_margin['value'].apply(lambda x: f'{x:.2f}%')

			net_profit_thismonth['catogory']='Net Profit'
			net_profit_thismonth['date']=lostToMonths[-1]
			net_profit_thismonth.fillna(0,inplace=True)
			print(net_profit_thismonth.head())
			# net_profit_thismonth['value']=net_profit_thismonth['value'].apply(lambda x: f'{x:.2f}%')

			df_thismonth=pd.concat([current_month,total_gross_profit,total_gross_margin,net_profit_thismonth,net_margin]).reset_index(drop=True)

			df_thismonth=df_thismonth.pivot_table(index='catogory',columns='date',values='value',aggfunc='sum',fill_value=0,observed=True,sort=False).reset_index().replace([np.inf, -np.inf],0)

			merged_df=last_month.merge(df_thismonth,how='left',on='catogory').fillna(0)
			
			merged_df['MOM']=(((merged_df['June']-merged_df['May'])/merged_df['May'])*100).round(0)
			print(merged_df.head())
			df=q_df.merge(merged_df,how='right',on='catogory')
			df.fillna(0,inplace=True)
			print(df.columns)
			return df.to_dict(orient='records')

		elif catogory=='Region':
			
			gp_df=futures[0].result()
			gp_df['catogory']=gp_df.item_code.str.split('-').str[0].str.strip().str.title()
			gp_df['item_name'] = gp_df.item_code.str.split('-').str[1].str.strip().str.title()
			two_month=gp_df


			gp_df.posting_date=pd.to_datetime(gp_df.posting_date)

			# %%
			two_month.posting_date=two_month.posting_date.sort_values(ascending=False)


			# %%
			two_month['date']=two_month.posting_date.dt.month_name()

			# %%
			two_month=two_month[two_month.posting_date>=(now-timedelta(days=31)).strftime("%Y-%m-01")]

			two_month_gp=two_month.groupby(['location_zone','date'])[['gross_profit','base_net_amount']].sum().sort_values('gross_profit',ascending=False).reset_index()
			two_month_gp['GM']=((two_month_gp['gross_profit']/two_month_gp['base_net_amount'])*100).round(2)

			# %%
			two_month_gp.rename(columns={'gross_profit':'value'},inplace=True)


			last_month=two_month_gp


			# %%
			last_month=last_month.pivot_table(index=['location_zone'],columns='date',values='value',aggfunc='sum',fill_value=0,observed=True,sort=False).reset_index()

			# %%
			gp_df=gp_df[gp_df.posting_date.isin(x)]
			gp=gp_df.groupby(['location_zone'])[['gross_profit','base_net_amount']].sum().sort_values('gross_profit',ascending=False).reset_index()
			gp['GM']=((gp['gross_profit']/gp['base_net_amount'])*100).round(2)
			# %%
		

			# %%
			gp.rename(columns={'gross_profit':'value'},inplace=True)

			# %%
			df=gp

			# %%
			q_df=df.pivot_table(index='location_zone',values='value',aggfunc='sum',fill_value=0,observed=True,sort=False).reset_index()

			q_df.rename(columns={'value':filters.get('quarterly')},inplace=True)

			# %%
			lostToMonths=(pd.date_range(start=now-timedelta(days=31),end=now)).month_name().unique()

			
			# %%
			current_month=two_month[two_month.date.isin([lostToMonths[-1]])]

			# %%
			current_month=current_month.groupby(['location_zone','date'])[['gross_profit','base_net_amount']].sum().sort_values('gross_profit',ascending=False).reset_index()
			current_month['GM']=((current_month['gross_profit']/current_month['base_net_amount'])*100).round(2)

			# %%
			current_month.rename(columns={'gross_profit':'value'},inplace=True)
	
			# %%

			df_thismonth=current_month
			df_thismonth['location_zone']=last_month['location_zone']
			df_thismonth['date']=lostToMonths[-1]
			df_thismonth.fillna(0,inplace=True)
			df_thismonth=df_thismonth.pivot_table(index='location_zone',columns='date',values='value',aggfunc='sum',fill_value=0,observed=True,sort=False).reset_index().replace([np.inf, -np.inf],0)
			merged_df=last_month.merge(df_thismonth,how='left',on='location_zone').fillna(0)
			print(merged_df.head())
			merged_df['MOM']=(((merged_df['June']-merged_df['May'])/merged_df['May'])*100).round(0)
			print(merged_df.head())
			df=q_df.merge(merged_df,how='right',on='location_zone')
			df.fillna(0,inplace=True)
			print(df.columns)
			return df.to_dict(orient='records')

		else:
      
			gp_df=futures[0].result()
			gp_df['catogory']=gp_df.item_code.str.split('-').str[0].str.strip().str.title()
			gp_df['item_name'] = gp_df.item_code.str.split('-').str[1].str.strip().str.title()
			two_month=gp_df


			gp_df.posting_date=pd.to_datetime(gp_df.posting_date)

			# %%
			two_month.posting_date=two_month.posting_date.sort_values(ascending=False)


			# %%
			two_month['date']=two_month.posting_date.dt.month_name()

			# %%
			two_month=two_month[two_month.posting_date>=(now-timedelta(days=31)).strftime("%Y-%m-01")]

			two_month_gp=two_month.groupby(['item_name','catogory','date'])[['gross_profit','base_net_amount']].sum().sort_values('gross_profit',ascending=False).reset_index()
			two_month_gp['GM']=((two_month_gp['gross_profit']/two_month_gp['base_net_amount'])*100).round(2)

			# %%
			two_month_gp.rename(columns={'gross_profit':'value'},inplace=True)


			last_month=two_month_gp


			# %%
			last_month=last_month.pivot_table(index=['item_name','catogory'],columns='date',values='value',aggfunc='sum',fill_value=0,observed=True,sort=False).reset_index()

			# %%
			gp_df=gp_df[gp_df.posting_date.isin(x)]
			gp=gp_df.groupby(['item_name','catogory'])[['gross_profit','base_net_amount']].sum().sort_values('gross_profit',ascending=False).reset_index()
			gp['GM']=((gp['gross_profit']/gp['base_net_amount'])*100).round(2)
			# %%


			# %%
			gp.rename(columns={'gross_profit':'value'},inplace=True)

			# %%
			df=gp

			# %%
			q_df=df.pivot_table(index=['item_name','catogory'],values='value',aggfunc='sum',fill_value=0,observed=True,sort=False).reset_index()

			q_df.rename(columns={'value':filters.get('quarterly')},inplace=True)

			# %%
			lostToMonths=(pd.date_range(start=now-timedelta(days=31),end=now)).month_name().unique()


			# %%
			current_month=two_month[two_month.date.isin([lostToMonths[-1]])]

			# %%
			current_month=current_month.groupby(['item_name','catogory','date'])[['gross_profit','base_net_amount']].sum().sort_values('gross_profit',ascending=False).reset_index()
			current_month['GM']=((current_month['gross_profit']/current_month['base_net_amount'])*100).round(2)

			# %%
			current_month.rename(columns={'gross_profit':'value'},inplace=True)

			# %%

			df_thismonth=current_month
			df_thismonth['item_name']=last_month['item_name']
			df_thismonth['date']=lostToMonths[-1]
			df_thismonth.fillna(0,inplace=True)
			df_thismonth=df_thismonth.pivot_table(index=['item_name','catogory'],columns='date',values='value',aggfunc='sum',fill_value=0,observed=True,sort=False).reset_index().replace([np.inf, -np.inf],0)
			df_thismonth.drop(['catogory'],axis=1,inplace=True)
			merged_df=last_month.merge(df_thismonth,how='left',on=['item_name']).fillna(0)
			
			merged_df['MOM']=(((merged_df['June']-merged_df['May'])/merged_df['May'])*100).round(0)
			df=q_df.merge(merged_df,how='left',on=['item_name','catogory'])
			df.fillna(0,inplace=True)
			print(df.head())

			return df.to_dict(orient='records')

  
  
  
  