# Copyright (c) 2025, Aerele Technologies Private Limited and contributors
# For license information, please see license.txt
#ddadsasddsasda
import frappe
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
months = pd.date_range(end=pd.Timestamp.today(), periods=13, freq='MS').strftime('%Y-%B').tolist()[::-1]
site = frappe.local.site
def execute(filters=None):
    data=get_data(filters)
    columns=get_columns(data)
 
    return columns, data


def get_columns(data):
    base_columns = [
        {"fieldname": "item_code", "label": "Item Code", "fieldtype": "Data", "width": 100},
        {"fieldname": "item_name", "label": "Item Name", "fieldtype": "Data", "width": 300},
        {"fieldname": "brand", "label": "Brand", "fieldtype": "Data", "width": 120},
        {"fieldname": "country_of_origin", "label": "Origin", "fieldtype": "Data", "width": 120},
        {"fieldname": "tire_type", "label": "Category1", "fieldtype": "Data", "width": 120},
        {"fieldname": "tire_segment", "label": "Category2", "fieldtype": "Data", "width": 120},
        {"fieldname": "stock", "label": "ARABIAN STOCK", "fieldtype": "int", "width": 120},
        {"fieldname": "cy_qty", "label": "Container Yard", "fieldtype": "int", "width": 120},
        {"fieldname": "shipped", "label": "Shipped", "fieldtype": "int", "width": 120},
        {"fieldname": "pending", "label": "Pending", "fieldtype": "int", "width": 120},
        {"fieldname": "price", "label": "Price", "fieldtype": "Currency", "width": 120},
    ]

    if not data:
        return base_columns

    # detect dynamic month columns
    all_keys = {key for row in data for key in row.keys()}
    dynamic_keys = [
        k for k in all_keys 
        if k not in ("item_code", "item_name", "brand", "country_of_origin",
                     "tire_type", "tire_segment", "stock", "cy_qty", "shipped", "pending", "price")
    ]

    # ensure month order follows same as global months list
    ordered_dynamic_keys = [m for m in months if m in dynamic_keys]

    month_columns = [
        {"fieldname": m, "label": f"Sales ({m})", "fieldtype": "int", "width": 130}
        for m in ordered_dynamic_keys
    ]

    return base_columns + month_columns

def get_data(filters):
    brand = filters.get("brand")
    production_year = filters.get("production_year")
    price_list = filters.get("price_list")

    queries = [
        # items
        f"""WITH stock AS (
                SELECT b.item_code, SUM(b.actual_qty) AS stock_qty
                FROM tabBin AS b
                WHERE b.warehouse NOT IN (
                    SELECT default_container_yard_warehouse FROM tabCompany
                )
                GROUP BY 1
            ),
            container_yeard AS (
                SELECT b.item_code, SUM(b.actual_qty) AS cy_qty
                FROM tabBin AS b
                WHERE b.warehouse IN (
                    SELECT default_container_yard_warehouse FROM tabCompany
                )
                GROUP BY 1
            ),
            price_list AS (
                SELECT ip.item_code, ip.price_list_rate
                FROM `tabPrice List` pl
                LEFT JOIN `tabItem Price` ip ON pl.name = ip.price_list
                WHERE pl.selling = 1
                AND pl.name = '{price_list}'
                AND ip.valid_from = (
                    SELECT MAX(ip2.valid_from)
                    FROM `tabItem Price` ip2
                    WHERE ip2.item_code = ip.item_code
                    AND ip2.price_list = ip.price_list
                )
                AND ip.production_year = '{production_year}'
            )
            SELECT 
                i.name AS item_code,
                i.item_name,
                i.brand,
                i.country_of_origin,
                i.tire_type,
                i.tire_segment,
                COALESCE(s.stock_qty,0) AS stock,
                COALESCE(c.cy_qty,0) AS cy_qty,
                COALESCE(p.price_list_rate,0) AS price
            FROM tabItem i
            LEFT JOIN stock s ON s.item_code = i.name 
            LEFT JOIN price_list p ON p.item_code = i.name
            LEFT JOIN container_yeard c ON c.item_code = i.name
            WHERE i.brand = '{brand}'
        """,

        # sales
        f"""SELECT 
                i.name AS item_code,
                DATE_FORMAT(sle.posting_date, '%%Y-%%M') AS date,
                SUM(NULLIF(sle.actual_qty,0)*-1) AS qty
            FROM `tabStock Ledger Entry` sle 
            LEFT JOIN `tabItem` i ON i.name = sle.item_code 
            WHERE sle.is_cancelled = 0
            AND sle.voucher_type IN ('Sales Invoice','Delivery Note')
            AND sle.posting_date >= DATE_SUB(CURDATE(), INTERVAL 13 MONTH)
            AND i.brand = '{brand}'
            GROUP BY 1,2
        """,

        # pending
        f"""WITH purchase_order_item_pending AS (
                SELECT
                    po.name AS purchase_order,
                    po.transaction_date,
                    po.supplier,
                    poi.item_code,
                    SUM(poi.qty) AS qty
                FROM `tabPurchase Order Item` poi
                INNER JOIN `tabPurchase Order` po ON poi.parent = po.name
                WHERE po.docstatus = 1
                AND poi.docstatus = 1
                AND po.closed = 0
                AND po.status NOT IN ('Completed', 'Closed')
                GROUP BY po.name, po.transaction_date, po.supplier, poi.item_code
                UNION ALL
                SELECT
                    pii.purchase_order,
                    po.transaction_date,
                    po.supplier,
                    pii.item_code,
                    SUM(pii.qty) * -1 AS qty
                FROM `tabPurchase Invoice Item` pii
                INNER JOIN `tabPurchase Order` po ON pii.purchase_order = po.name
                WHERE po.docstatus = 1
                AND pii.docstatus IN (0, 1)
                AND po.closed = 0
                AND po.status NOT IN ('Completed', 'Closed')
                GROUP BY pii.purchase_order, po.transaction_date, po.supplier, pii.item_code
            )
            SELECT
                i.name AS item_code,
                SUM(poip.qty) AS pending
            FROM purchase_order_item_pending poip
            LEFT JOIN tabItem i ON i.name = poip.item_code 
            WHERE i.brand = '{brand}'
            GROUP BY item_code
            HAVING SUM(qty) > 0
        """,

        # shipped
        f"""SELECT
                i.name AS item_code,
                SUM(pii.qty) AS shipped
            FROM `tabPurchase Invoice Item` pii
            INNER JOIN `tabPurchase Invoice` pi ON pii.parent = pi.name
            LEFT JOIN tabItem i ON i.name = pii.item_code
            WHERE pi.docstatus IN (0, 1)
            AND pii.docstatus IN (0, 1)
            AND pi.update_stock = 0
            AND pi.is_return = 0
            AND pii.qty > 0
            AND pii.item_code IS NOT NULL
            AND pi.title NOT IN (
                SELECT title
                FROM `tabPurchase Receipt`
                WHERE docstatus = 1
                AND is_return = 0
            )
            AND i.brand = '{brand}'
            GROUP BY item_code
        """
    ]

    # ✅ Helper: ensure non-empty DataFrame
    def safe_df(result, columns, key="item_code"):
        df = pd.DataFrame([dict(row) for row in result])
        if df.empty:
            df = pd.DataFrame([{col: 0 for col in columns}])
            df[key] = "EMPTY"
        return df

    # ✅ Thread-safe query runner
    def run_query(sql):
        frappe.init(site=site)
        frappe.connect()
        try:
            return frappe.db.sql(sql, as_dict=True)
        finally:
            frappe.destroy()

    # ✅ Parallel query execution
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(run_query, queries))

    # ✅ Apply safe_df to all results
    items   = safe_df(results[0], ["item_code","item_name","brand","country_of_origin","tire_type","tire_segment","stock","cy_qty","price"])
    sales   = safe_df(results[1], ["item_code","date","qty"])
    pending = safe_df(results[2], ["item_code","pending"])
    shipped = safe_df(results[3], ["item_code","shipped"])

    # ✅ Handle sales pivot safely
    sales_pivot = sales.pivot_table(
        index="item_code", columns="date", values="qty", aggfunc="sum", fill_value=0
    ).reset_index()

    for m in months:
        if m not in sales_pivot.columns:
            sales_pivot[m] = 0

    sales_pivot = sales_pivot[["item_code"] + months]

    # ✅ Merge all
    merge_df = (
        items.merge(pending, how="left", on="item_code")
             .merge(shipped, how="left", on="item_code")
             .merge(sales_pivot, how="left", on="item_code")
             .fillna(0)
    )

    # Optional: remove dummy record
    merge_df = merge_df[merge_df["item_code"] != "EMPTY"]

    return merge_df.to_dict(orient="records")
