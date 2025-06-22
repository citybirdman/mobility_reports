# Copyright (c) 2025, Aerele Technologies Private Limited and contributors
# For license information, please see license.txt

import pandas as pd # type: ignore
from datetime import datetime
now=datetime.now()



def execute(filters=None):
	data=get_data()
	columns=get_columns(data)
	return columns, data


def get_columns(data):

	columns = [
     
     	{
			"fieldname": "posting_date",
			"label": "Posting Date",
			"fieldtype": "Date",
			"width": 120
		},
      	{
			"fieldname": "catogory",
			"label": "Catogory",
			"fieldtype": "Data",
			"width": 100
		},
		{
			"fieldname": "item_name",
			"label": "Item Name",
			"fieldtype": "Data",
			"width": 120
		},
		{
			"fieldname": "customer_name",
			"label": "Customer Name",
			"fieldtype": "Data",
			"width": 150
		},
		{
			"fieldname": "territory",
			"label": "Country",
			"fieldtype": "Data",
			"width": 120
		},
		{
			"fieldname": "custom_location_zone",
			"label": "Location Zone",
			"fieldtype": "Data",
			"width": 120
		},
		{
			"fieldname": "custom_Sales_channel",
			"label": "Sales Channel",
			"fieldtype": "Data",
			"width": 120
		},
		{
			"fieldname": "custom_status",
			"label": "Status",
			"fieldtype": "Data",
			"width": 120
		},
		{
			"fieldname": "custom_order_method",
			"label": "Order Method",
			"fieldtype": "Data",
			"width": 120
        },
	
		{
			"fieldname": "qty",
			"label": "Qty",
			"fieldtype": "Int",
			"width": 120
		},
		{
			"fieldname": "base_rate",
			"label": "Rate",
			"fieldtype": "Float",
			"width": 120	
		},
		{
			"fieldname": "base_amount",
			"label": "Amount",
			"fieldtype": "Float",
			"width": 120
		},
  		{
			"fieldname": "custom_shipping_cost",
			"label": "Shipping Cost",
			"fieldtype": "Float",
			"width": 120
		},
		{
			"fieldname": "amount_after_vat",
			"label": "Amount After VAT",
			"fieldtype": "Float",
			"width": 120
		},
	
	]
	return columns



def get_data():

	df=pd.read_csv("https://www.dropbox.com/scl/fi/yvz5bm17177jdu0ok4z3v/OMG-Sales-24-2024-Sales.csv?rlkey=fe05jv6pp86h4ma6aehe6w8os&st=x1cgkd35&dl=1")
	df['posting_date'] = pd.to_datetime(df['posting_date'], format='mixed', dayfirst=True).dt.strftime('%Y-%m-%d')	

	df.territory=df.territory.str.strip().str.upper()
	df.custom_location_zone=df.custom_location_zone.str.strip().str.upper()
	df.custom_Sales_channel=df.custom_Sales_channel.str.strip().str.title()
	df.custom_invoice_number=df.custom_invoice_number.str.strip().str.title()
	df.custom_status=df.custom_status.str.strip().str.title()
	df.custom_order_method=df.custom_order_method.str.strip().str.title()
	df.customer_name=df.customer_name.str.strip().str.title()
	df.item_code=df.item_code.str.strip().str.title()
	df.custom_payment_status=df.custom_payment_status.str.strip().str.title()
	df['catogory']=df.item_code.str.split('-').str[0].str.strip().str.title()
	df['item_name'] = df.item_code.str.split('-').str[1].str.strip().str.title()
	df=df[['catogory','item_name','posting_date','customer_name','custom_location_zone','custom_Sales_channel','territory','custom_status','custom_order_method','qty','base_rate','base_amount','amount_after_vat','custom_shipping_cost']]
	df=df.groupby(['catogory', 'item_name', 'posting_date', 'customer_name', 'custom_location_zone', 'custom_Sales_channel', 'territory','custom_status','custom_order_method']).sum().reset_index()
	
	return df.to_dict(orient='records')