import frappe
import pandas as pd
import numpy as np
def execute(filters=None):
	data = get_data()
	columns = get_columns(data)
	return columns, data

def get_columns(data):
	"""Dynamically generate columns based on available years in the dataset."""
	base_columns = [
		{"fieldname": "Order No.", "label": "Order No", "fieldtype": "Data", "width": 200},
		{"fieldname": "ETD", "label": "ETD", "fieldtype": "Date", "width": 200},
	]

	
	return base_columns

def get_data():
	df=pd.read_excel('https://www.dropbox.com/scl/fi/cxq3am1pdgot6hbacd9q5/ARABIAN-TIRES-2025-Shipping-Report.xlsx?rlkey=3do7euz49mg3m3js4dd7xyqz5&dl=1',header=4,usecols=['Order No.','ETD'])
	df=df[~df['ETD'].isna()]
                                        
 
	return df.to_dict(orient='records')