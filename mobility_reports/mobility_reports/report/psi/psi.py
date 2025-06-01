# Copyright (c) 2025, Aerele Technologies Private Limited and contributors
# For license information, please see license.txt

import frappe
import pandas as pd # type: ignore
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine # type: ignore
import requests 
now=datetime.now()

def execute(filters=None):
	columns, data = [], []
	return columns, data


def get_data():
	ssl_url = "https://www.dropbox.com/scl/fi/1hj515q7rykj0l2urpytn/omg.pem?rlkey=3brhxb9x52v23myeegt85983a&st=31ostnmu&dl=1"
	response = requests.get(ssl_url)

	cert_path = "n1-ksa.frappe.cloud.omg.pem"
	with open(cert_path, "wb") as f:
		f.write(response.content)

	ssl_args = {"ssl": {"ca": cert_path}}
	connection_string = f"mysql+pymysql://174a179b828f397:f0f036846ffcf44c3def@n1-ksa.frappe.cloud:3306/_99a43d5c723190d4"
	engine = create_engine(connection_string, connect_args=ssl_args)

	queries=[
		# Purchase
		"""select DATE_FORMAT(posting_date,'%%M')as date ,sum(total_qty)as qty, sum(grand_total)as amount  from `tabPurchase Invoice` 
		where status != 'Cancelled' and docstatus=1 and DATE_FORMAT(posting_date,'%%Y-01-01') >= DATE_FORMAT(Current_date(),'%%Y-01-01')
		group by date""",

		# Sales
		"""select DATE_FORMAT(posting_date,'%%M')as date,sum(total_qty)as qty,sum(grand_total)as amount from `tabSales Invoice`
		where status != 'Cancelled' and docstatus=1 and DATE_FORMAT(posting_date,'%%Y-01-01') >= DATE_FORMAT(Current_date(),'%%Y-01-01')
		group by 1""",

		# inventory
		"""SELECT 
    DATE_FORMAT(posting_date,'%%M') as date,
    SUM(SUM(actual_qty)) OVER (ORDER BY posting_date) as qty,
    SUM(SUM(stock_value_difference)) OVER (ORDER BY posting_date) as amount
	FROM `tabStock Ledger Entry`
	where is_cancelled = 0
	GROUP BY date
	ORDER BY posting_date;"""

	]

	def run_query(sql):
		return pd.read_sql(sql,engine)
	
	with ThreadPoolExecutor(max_workers=3) as executor:
		futures = [executor.submit(run_query, q) for q in queries]

	print(futures)
	print(futures[0].head())


