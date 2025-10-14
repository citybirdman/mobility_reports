# Copyright (c) 2025, Aerele Technologies Private Limited and contributors
# For license information, please see license.txt

import frappe
import pandas as pd 
from sqlalchemy import create_engine
import numpy as np
from datetime import datetime
import requests
ssl_url = "https://www.dropbox.com/scl/fi/1hj515q7rykj0l2urpytn/omg.pem?rlkey=3brhxb9x52v23myeegt85983a&st=31ostnmu&dl=1"
response = requests.get(ssl_url)

cert_path = "n1-ksa.frappe.cloud.omg.pem"
with open(cert_path, "wb") as f:
    f.write(response.content)

ssl_args = {"ssl": {"ca": cert_path}}
connection_string = f"mysql+pymysql://8c48725a15b2a9c:575f0e3b1e05fdb4f4ae@n1-ksa.frappe.cloud:3306/_61c733e77de10d32"
engine = create_engine(connection_string, connect_args=ssl_args)


def execute(filters=None):
	data=get_data(filters)
	columns=get_columns(data)
	return columns, data
def get_columns(data):
    return [
        {
            "fieldname": "item_code",
            "label": "Item Code",
            "fieldtype": "data",
            "options": "Item",
            "width": 100
        },
        {
            "fieldname": "item_name",
            "label": "Item Name",
            "fieldtype": "data",
            "width": 300
        },
        {
            "fieldname": "brand",
            "label": "Brand",
            "fieldtype": "data",
            "options": "Brand",
            "width": 140
        },
        {
            "fieldname": "tire_size",
            "label": "Tire Size",
            "fieldtype": "data",
            "options": "Tire Size",
            "width": 140
        },
        {
            "fieldname": "ply_rating",
            "label": "PR",
            "fieldtype": "Data",
            "width": 100
        },
        {
            "fieldname": "production_year",
            "label": "Production Year",
            "fieldtype": "data",
            "options": "Production Year",
            "width": 100
        }
    ]
def get_data(filters): 
    warehouse = filters.get("warehouse")
    brands = filters.get("brand") or []
    qty = filters.get("qty") or 0

    # Ensure brands are SQL-safe: quoted and comma-separated
    if isinstance(brands, str):
        # if it's coming from MultiSelect as a comma-separated string
        brands = [b.strip() for b in brands.split(",")]
    brand_list = ", ".join([f"'{b}'" for b in brands])
    warehouse_maping={
		'Jeddah':'مستودعات جدة الرئيسية (الخمرة)',
		'Dammam Main':'مستودع الدمام الرئيسي',
		'Riyadh':'مستودع الرياض'
	}
    warehouse=warehouse_maping.get(warehouse)
    print(warehouse)
    # Build the SQL with placeholders to prevent SQL injection
    sql = f"""
    SELECT
        stock_ledger_entry.item_code ,
        item.item_name ,
        item.brand ,
        item.tire_size ,
        item.ply_rating ,
        stock_ledger_entry.production_year 
    FROM
        (
        SELECT
            item_code,
            production_year,
            w.warehouse_name AS warehouse,
            SUM(actual_qty) AS actual_qty
        FROM
            `tabStock Ledger Entry`
        join tabWarehouse w on warehouse=w.name
        WHERE
            is_cancelled = 0
            AND w.warehouse_name = '{warehouse}'
        GROUP BY
            item_code, production_year, warehouse
        HAVING
            SUM(actual_qty) > 0
        ) stock_ledger_entry
    LEFT JOIN
        (
        SELECT
            item_code,
            production_year,
            SUM(qty_to_deliver) AS qty_to_deliver
        FROM
            (
            SELECT
                sales_order_item.item_code,
                sales_order_item.production_year,
                IF(SUM(sales_order_item.qty - sales_order_item.delivered_qty) > 0,
                   SUM(sales_order_item.qty - sales_order_item.delivered_qty), 0) AS qty_to_deliver
            FROM
                `tabSales Order Item` sales_order_item
            INNER JOIN
                `tabSales Order` sales_order
                ON sales_order_item.parent = sales_order.name
			INNER JOIN tabWarehouse w on sales_order.set_warehouse=w.name
            WHERE
                sales_order.docstatus IN (0, 1)
                AND sales_order_item.docstatus IN (0, 1)
                AND sales_order.closed = 0
                AND sales_order.status NOT IN ('Completed', 'Closed')
                AND (sales_order_item.qty - sales_order_item.delivered_qty) > 0
                AND w.warehouse_name = '{warehouse}'
            GROUP BY
                sales_order_item.item_code,
                sales_order_item.production_year
            ) sales_order_item
        GROUP BY
            item_code, production_year
        ) sales_order_item_pending
    ON
        stock_ledger_entry.item_code = sales_order_item_pending.item_code
        AND stock_ledger_entry.production_year = sales_order_item_pending.production_year
    INNER JOIN
        `tabItem` item
        ON stock_ledger_entry.item_code = item.name
        AND item.is_stock_item = 1
    WHERE
        item.brand IN ({brand_list})
        AND stock_ledger_entry.actual_qty - IFNULL(sales_order_item_pending.qty_to_deliver, 0) >= {qty}
    ORDER BY
        item.brand,
        item.sorting_code
    """
    data = pd.read_sql(sql, con=engine)
    print(data)
    return data.to_dict("records")
