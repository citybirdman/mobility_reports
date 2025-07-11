import pandas as pd  # type: ignore
import frappe

def execute(filters=None):
		data = get_data(filters)
		columns = get_columns(data)
		return columns, data

def get_columns(data):
		base_columns = [
			{"fieldname": "item_code", "label": "Item Code", "fieldtype": "Data", "width": 100},
			{"fieldname": "item_name", "label": "Item Name", "fieldtype": "Data", "width": 300},
			{"fieldname": "brand", "label": "Brand", "fieldtype": "Data", "width": 120},
			{"fieldname": "actual_qty", "label": "Qty", "fieldtype": "int", "width": 120},
			{"fieldname": "qty_to_deliver", "label": "Qty To Deliver", "fieldtype": "int", "width": 120},
			{"fieldname": "available_qty", "label": "Available Qty", "fieldtype": "int", "width": 120},
			{"fieldname": "price_list_rate", "label": "Price List Rate", "fieldtype": "float", "width": 120},
		]
		if data:
			# Extract brand names dynamically
			fileds = {key for row in data for key in row.keys() if key not in("item_code", "item_name", "brand", "actual_qty", "qty_to_deliver", "available_qty", "price_list_rate")}

			# Append brand-specific columns
			brand_columns = [
				{"fieldname": filed, "label": f"Sales({filed})", "fieldtype": "int", "width": 150}
				for filed in fileds
			]
			base_columns.extend(brand_columns)
		return base_columns 
def get_data(filters):
	if not filters.get("from_date") or not filters.get("to_date"):
		frappe.throw("Both 'From Date' and 'To Date' filters are required.")

	from_date = filters["from_date"]
	to_date = filters["to_date"]

	sql_params = {
		"from_date": from_date,
		"to_date": to_date
	}

	queries = [
		# First Query - Inventory & Order Data
		f"""
		WITH
			stock_ledger_entry AS (
				SELECT
				modified,
					item_code,
					SUM(actual_qty) AS actual_qty
				FROM `tabStock Ledger Entry`
				WHERE is_cancelled = 0
				AND posting_date <= %(to_date)s
				GROUP BY item_code
				HAVING SUM(actual_qty) > 0
			),

			sales_order_item AS (
				SELECT
					item_code,
					SUM(qty_to_deliver) AS qty_to_deliver
				FROM (
					SELECT
						sales_order.name AS sales_order,
						sales_order_item.item_code,
						IF(SUM(sales_order_item.qty - sales_order_item.delivered_qty) > 0, 
							SUM(sales_order_item.qty - sales_order_item.delivered_qty), 0) AS qty_to_deliver
					FROM `tabSales Order Item` sales_order_item
					INNER JOIN `tabSales Order` sales_order
						ON sales_order_item.parent = sales_order.name
					WHERE
						sales_order.docstatus = 1
						AND sales_order_item.docstatus = 1
						AND sales_order.status NOT IN ('Completed', 'Closed')
						AND sales_order_item.qty - sales_order_item.delivered_qty > 0
					GROUP BY sales_order.name, sales_order_item.item_code
				) sales_order_item
				GROUP BY item_code
			),

			item_price AS (
				SELECT
					item_code,
					price_list_rate
				FROM `tabItem Price`
				WHERE selling = 1
					AND price_list IN (
						SELECT value
						FROM `tabSingles`
						WHERE doctype = 'Selling Settings'
							AND field = 'selling_price_list'
					)
			)

		SELECT
			stock_ledger_entry.item_code,
			stock_ledger_entry.modified,
			item.item_name,
			item.brand,
			stock_ledger_entry.actual_qty,
			IFNULL(sales_order_item.qty_to_deliver, 0) AS qty_to_deliver,
			stock_ledger_entry.actual_qty - IFNULL(sales_order_item.qty_to_deliver, 0) AS available_qty,
			item_price.price_list_rate
		FROM stock_ledger_entry
		INNER JOIN `tabItem` item ON stock_ledger_entry.item_code = item.name
		LEFT JOIN item_price ON stock_ledger_entry.item_code = item_price.item_code
		LEFT JOIN sales_order_item ON stock_ledger_entry.item_code = sales_order_item.item_code
		WHERE item.is_stock_item = 1
		ORDER BY item.brand
		""",

		f"""
		SELECT
			sle.item_code,
			item.item_name,
			DATE_FORMAT(sle.posting_date, '%%Y-%%m') as date, 
			SUM(sle.actual_qty) * -1 AS total_sold
		FROM `tabStock Ledger Entry` sle
		JOIN `tabItem` item ON sle.item_code = item.name
		WHERE
			sle.voucher_type IN ('Sales Invoice', 'Delivery Note')
			AND sle.is_cancelled = 0
			AND sle.posting_date BETWEEN %(from_date)s AND %(to_date)s
		GROUP BY sle.item_code, item.item_name, DATE_FORMAT(sle.posting_date, '%%Y-%%m')

		"""
	]	

	result = [frappe.db.sql(q, sql_params, as_dict=True) for q in queries]
	data_stock = [dict(row) for row in result[0]]
	data_sales = [dict(row) for row in result[1]]

	stock = pd.DataFrame(data_stock)
	sales = pd.DataFrame(data_sales)
	
	
	stock=stock.sort_values('modified',ascending=False).drop_duplicates(['item_code', 'item_name', 'brand','actual_qty',], keep='first')
	merged_df = sales.merge(stock, on=["item_code", "item_name"], how="inner")
	pivoted_df = merged_df.pivot_table(
		index=["item_code", "item_name", "brand", "actual_qty", "qty_to_deliver", "available_qty", "price_list_rate"],
		columns="date",
		values="total_sold",
		aggfunc="sum"
	).fillna(0).reset_index()
	pivoted_df.columns.name = None
	priority_cols = ['item_code', 'item_name', 'brand', 'actual_qty', 'qty_to_deliver', 'available_qty', 'price_list_rate']

	month_cols = [col for col in pivoted_df.columns if col not in priority_cols]

	month_cols_sorted = sorted(month_cols, key=lambda x: pd.to_datetime(x, format='%Y-%m'))
	final_cols = priority_cols + month_cols_sorted
	pivoted_df=pivoted_df[final_cols]
	return pivoted_df.to_dict(orient="records")


	