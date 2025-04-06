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
		{"fieldname": "item_code", "label": "Item Code", "fieldtype": "Data", "width": 200},
		{"fieldname": "item_name", "label": "Item Name", "fieldtype": "Data", "width": 200},
		{"fieldname": "brand", "label": "Brand", "fieldtype": "Data", "width": 200},
		{"fieldname": "country_of_origin", "label": "Country", "fieldtype": "Data", "width": 200},
		{"fieldname": "tire_size", "label": "Tire Size", "fieldtype": "Data", "width": 200},
		{"fieldname": "ply_rating", "label": "Ply Rating", "fieldtype": "Data", "width": 200},
		{"fieldname": "tire_pattern", "label": "Tire Pattern", "fieldtype": "Data", "width": 200},
		{"fieldname": "weight_per_unit", "label": "Weight", "fieldtype": "Float", "width": 200},
		{"fieldname": "stuffing_volume", "label": "Volume", "fieldtype": "Float", "width": 200}
	]

	years = sorted(set(int(col.split("_")[-1]) for col in data[0].keys() if col.startswith("QTY_")))

	# Add dynamic year columns
	for year in years:
		base_columns.append({"fieldname": f"QTY_{year}", "label": f"Quantity {year}", "fieldtype": "Int", "width": 100})
		base_columns.append({"fieldname": f"FOB_{year}", "label": f"FOB {year}", "fieldtype": "Currency", "width": 120})

	return base_columns

def get_data():
	"""Fetches and processes data from Frappe database and returns a structured dictionary list."""
	query = """
	WITH new_table AS (
		SELECT 
			item_code, item_name, brand, item_group, country_of_origin, tire_type,
			tire_size, weight_per_unit, stuffing_volume, ply_rating, tire_pattern
		FROM `tabItem`
		WHERE item_group = 'Tires'
	)
	SELECT 
		new_table.item_code, new_table.item_name, new_table.brand, new_table.item_group,
		new_table.country_of_origin, new_table.tire_type, new_table.tire_size,
		new_table.tire_pattern, new_table.ply_rating, new_table.weight_per_unit,
		new_table.stuffing_volume, purchase_item.parent, purchase.name,
		EXTRACT(YEAR FROM purchase.shipping_date) AS year,
		ROUND(SUM(qty), 0) AS QTY,
		ROUND(SUM(net_amount), 2) AS FOB
	FROM `tabPurchase Invoice Item` AS purchase_item
	JOIN `tabPurchase Invoice` AS purchase ON purchase.name = purchase_item.parent
	JOIN new_table ON new_table.item_code = purchase_item.item_code
	WHERE purchase.shipping_date >= '2021-01-01'
	GROUP BY new_table.item_code, new_table.item_name, new_table.brand, new_table.item_group,
			new_table.country_of_origin, new_table.tire_type, new_table.tire_size,
			new_table.tire_pattern, new_table.ply_rating, new_table.weight_per_unit,
			new_table.stuffing_volume, purchase_item.parent, purchase.name, year
	"""

	data = frappe.db.sql(query, as_dict=True)

	data = [dict(row) for row in data]

	df = pd.DataFrame(data)
	if df.empty:
		frappe.throw("No data available for the given query.")


	pivot_df = df.pivot_table(
		values=['QTY', 'FOB'],
		columns='year',
		index=['item_code', 'item_name', 'brand', 'country_of_origin', 'tire_size',
			'ply_rating', 'tire_pattern', 'weight_per_unit', 'stuffing_volume'],
		aggfunc={'QTY': 'sum', 'FOB': 'sum'},
		fill_value=0
	).reset_index()

	pivot_df.columns = ['_'.join(map(str, col)) if isinstance(col, tuple) else col for col in pivot_df.columns]

	pivot_df.rename(columns=lambda x: x.rstrip("_"), inplace=True)

	for year in df['year'].dropna().unique():
		qty_col = f"QTY_{int(year)}"
		fob_col = f"FOB_{int(year)}"
		if qty_col in pivot_df.columns and fob_col in pivot_df.columns:
			pivot_df[fob_col] = pivot_df[fob_col] / pivot_df[qty_col].replace('',0)
	pivot_df = pivot_df.fillna(0)
	new_names = [
    "item_code", "item_name", "brand", "country_of_origin", "tire_size", 
    "ply_rating", "tire_pattern", "weight_per_unit", "stuffing_volume",'FOB_2021','FOB_2022','FOB_2023','FOB_2024',
    'QTY_2021','QTY_2022','QTY_2023','QTY_2024'
		]
 
	pivot_df.columns = [new_names[i] if i < 100 else f"{col[0]}_{col[1]}" for i, col in enumerate(pivot_df.columns)]
	pivot_df.item_code=pivot_df.item_code.astype(str)
	pivot_df.item_name=pivot_df.item_name.astype(str)
	pivot_df.brand=pivot_df.brand.astype(str)
	pivot_df.country_of_origin=pivot_df.country_of_origin.astype(str)
	pivot_df.tire_size=pivot_df.tire_size.astype(str)
	pivot_df.ply_rating=pivot_df.ply_rating.astype(str)
	pivot_df.tire_pattern=pivot_df.tire_pattern.astype(str)
	pivot_df.weight_per_unit=pivot_df.weight_per_unit.astype(str)
	pivot_df.stuffing_volume=pivot_df.stuffing_volume.astype(str)
                                        
 
	return pivot_df.to_dict(orient='records')