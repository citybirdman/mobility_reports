import frappe
import pandas as pd # type: ignore
import requests # type: ignore
from sqlalchemy import create_engine # type: ignore
from datetime import datetime

def execute(filters=None):
    data = get_data(filters)
    columns = get_columns(data)
    return columns, data

def get_columns(data):
    # Define static columns
    base_columns = [
        {"fieldname": "date", "label": "Date", "fieldtype": "Data", "width": 120},
        {"fieldname": "purchasing_qty", "label": "Purchasing Qty", "fieldtype": "int", "width": 130},
        {"fieldname": "sales_qty", "label": "Sales Qty", "fieldtype": "int", "width": 120},
        {"fieldname": "AVG_cost", "label": "AVG Cost/KG", "fieldtype": "int", "width": 120},
        {"fieldname": "profitability", "label": "Profitability", "fieldtype": "int", "width": 120},
    	{"fieldname": "total_skus", "label": "Total SKUs", "fieldtype": "int", "width": 120},
        {"fieldname": "New_SKUs", "label": "New SKUs", "fieldtype": "int", "width": 120},
        {"fieldname": "unique_skus", "label": "Unique SKUs", "fieldtype": "int", "width": 120}
    ]


    
    return base_columns

def get_data(filters):
    years = pd.date_range("2022-01-01", "2024-12-31", freq="YE").strftime('%Y').tolist()
    months = pd.date_range("2025-01-01", "2025-12-01", freq="MS").strftime('%Y-%m').tolist()
    full_dates = years + months

    brand_filter = ""
    if filters and filters.get("brand"):
        brand = filters["brand"]
        brand_filter = f"AND item.brand ='{brand}' " 
    shipping_dropbox=pd.read_excel('https://www.dropbox.com/scl/fi/p94c8l1nfz6uyeso4szkc/ARABIAN-TIRES-2025-Shipping-Report.xlsx?rlkey=tpjtpmhtywpwaxed99o80oxys&dl=1',header=4,usecols=['Order No.','ETD'])
    shipping_dropbox=shipping_dropbox[~shipping_dropbox['ETD'].isna()]
    shipping_dropbox.rename(columns={'Order No.':'title'},inplace=True)
    ssl_url = "https://www.dropbox.com/scl/fi/qg6vaczygt2o572cplm8l/n1-ksa.frappe.cloud._arabian.pem?rlkey=a2gqvzfa997rp4az44z7uftq0&dl=1"
    response = requests.get(ssl_url)

    cert_path = "n1-ksa.frappe.cloud._arabian.pem"
    with open(cert_path, "wb") as f:
        f.write(response.content)

    ssl_args = {"ssl": {"ca": cert_path}}
    connection_string = f"mysql+pymysql://8c48725a15b2a9c:575f0e3b1e05fdb4f4ae@n1-ksa.frappe.cloud:3306/_61c733e77de10d32"
    engine = create_engine(connection_string, connect_args=ssl_args)
    
    pruchase=pd.read_sql(f"""
         SELECT pn.title,DATE_FORMAT(pn.shipping_date, '%%Y') AS date,pni.item_code, item.brand,
            item.ply_rating,item.tire_size,
            ROUND(SUM(pni.qty), 0) AS purchasing_qty,
            round(SUM(pni.net_amount),2) AS net_amount,
            round(SUM(pni.qty * item.weight_per_unit),2) AS weight_qty
        FROM `tabPurchase Invoice` pn
        JOIN `tabPurchase Invoice Item` pni ON pn.name = pni.parent
        JOIN `tabItem` item ON item.name = pni.item_code
        WHERE pn.shipping_date >= DATE_SUB(DATE_FORMAT(CURDATE(), '%%Y-01-01'), INTERVAL 3 YEAR)
            AND pn.shipping_date < DATE_FORMAT(CURDATE(), '%%Y-01-01')
            AND item.item_group = 'Tires'
            AND item.weight_per_unit > 0
            {brand_filter}
        GROUP BY item.brand, pni.item_code, pn.title, date

        UNION ALL

        SELECT pn.title,DATE_FORMAT(pn.shipping_date, '%%Y-%%m') AS date,pni.item_code, item.brand,
            item.ply_rating,item.tire_size,
            ROUND(SUM(pni.qty), 0) AS purchasing_qty,
            SUM(pni.net_amount) AS net_amount,
            SUM(pni.qty * item.weight_per_unit) AS purchasing_cost
        FROM `tabPurchase Invoice` pn
        JOIN `tabPurchase Invoice Item` pni ON pn.name = pni.parent
        JOIN `tabItem` item ON item.name = pni.item_code
        WHERE (pn.shipping_date >= DATE_FORMAT(CURDATE(), '%%Y-01-01') OR pn.shipping_date IS NULL)
            AND item.item_group = 'Tires'
            AND item.weight_per_unit > 0
           {brand_filter}
        GROUP BY pni.item_code, item.brand, pn.title, date
    """, engine )
    # pruchase= [dict(row) for row in pruchase]
    pruchase=pd.DataFrame(pruchase)
    if pruchase.empty :
        pruchase=pd.DataFrame(columns=['title','date','item_code','brand','ply_rating','tire_size','purchasing_qty','net_amount','weight_qty'])
        
    purchase_order = pd.read_sql(f"""
       SELECT min(po.schedule_date) AS date, pod.item_code, item.brand
        FROM `tabPurchase Order` po
        JOIN `tabPurchase Order Item` pod ON po.name = pod.parent
        JOIN `tabItem` item ON item.name = pod.item_code
        WHERE item.item_group = 'Tires'
            AND item.weight_per_unit > 0
            {brand_filter}
        GROUP BY item.brand, pod.item_code
                """,engine)
    # purchase_order   = [dict(row) for row in purchase_order]
    purchase_order=pd.DataFrame(purchase_order)
    if purchase_order.empty:
        purchase_order=pd.DataFrame(columns=['date','item_code','brand'])
    
    purchase=pruchase.merge(shipping_dropbox,how='left',on='title').reset_index()
   
    purchase.ETD=pd.to_datetime(purchase.ETD, format='%Y-%m').dt.strftime('%Y-%m')
    for index,row in purchase.iterrows():
        if row['date'] is None:
            purchase.at[index,'date']=row.ETD
    purchase=purchase[purchase.date!='2021'].reset_index(drop=True)
    
    pruchase=pruchase[~pruchase.date.isna()].reset_index(drop=True)
    # %%
    purchase_order['date'] = pd.to_datetime(purchase_order['date'],errors='coerce')

    # %%
    last3years = purchase_order[purchase_order['date'] < pd.to_datetime('2025-01-01',errors='coerce')]
    current_year=purchase_order[purchase_order['date'] >= pd.to_datetime('2025-01-01',errors='coerce')]


    # %%
    last3years.date=pd.to_datetime(last3years.date).dt.year
    current_year.date=pd.to_datetime(current_year.date).dt.strftime('%Y-%m')

    # %%
    last3years=last3years.sort_values(by=['date'])
    current_year=current_year.sort_values(by=['date'])

    # %%
    current_year=current_year.groupby(['date'])['brand'].count().reset_index()

    # %%
    current_year.rename(columns={'brand':'New_SKUs'},inplace=True)

    # %%
    last3years=last3years.groupby(['date'])['brand'].count().reset_index()
    last3years=last3years[last3years.date>=2022]
    last3years.rename(columns={'brand':'New_SKUs'},inplace=True)
    new_sku=pd.concat([last3years,current_year]).reset_index(drop=True)
    new_sku.date=new_sku.date.astype(str)

    # %%
    if pruchase.empty:
        purchase_pivot=pd.DataFrame(columns=['date','purchasing_qty','AVG_cost','total_skus','unique_skus','New_SKUs'])
        years = pd.date_range("2022-01-01", "2024-12-31", freq="YE").strftime('%Y').tolist()
        months = pd.date_range("2025-01-01", "2025-12-01", freq="MS").strftime('%Y-%m').tolist()
        full_dates = years + months
        purchase_pivot['date']=full_dates
        purchase_pivot.fillna(0,inplace=True)
    else:
       
        U_skus = purchase.drop_duplicates(subset=['date', 'ply_rating', 'tire_size']) \
                        .groupby('date') \
                        .size() \
                        .reset_index(name='unique_skus')
        U_skus.date=U_skus.date.astype(str)
        data = purchase.drop_duplicates(subset=['date', 'item_code']) \
                        .groupby('date') \
                        .size() \
                        .reset_index(name='total_skus')
        data.date=data.date.astype(str)
        purchase_1=purchase.pivot_table(index=['date'],values=['purchasing_qty','net_amount','weight_qty'],aggfunc='sum',fill_value=0,observed=False).reset_index()
        purchase_1['AVG_cost']=purchase_1['net_amount']/(purchase_1['weight_qty'])
        purchase_1=purchase_1[['date','purchasing_qty','AVG_cost']]
        purchase_pivot=purchase_1.merge(data,on='date').merge(U_skus,on='date').merge(new_sku,how='left',on='date').fillna(0) 
    # %%
    sle=pd.read_sql(f"""
                     SELECT DATE_FORMAT(sle.posting_date,'%%Y') AS date, item.brand, ROUND(SUM(sle.actual_qty)*-1,0) AS sales_qty 
        FROM `tabStock Ledger Entry` sle
        JOIN `tabItem` item ON sle.item_code = item.name
        WHERE sle.voucher_type IN ('Sales Invoice','Delivery Note')
            AND sle.is_cancelled = 0
            AND item.item_group = 'Tires'
            AND sle.posting_date >= DATE_SUB(DATE_FORMAT(CURDATE(), '%%Y-01-01'), INTERVAL 3 YEAR)
            AND sle.posting_date < DATE_FORMAT(CURDATE(), '%%Y-01-01')
            {brand_filter}
        GROUP BY date, item.brand

        UNION ALL

        SELECT DATE_FORMAT(sle.posting_date,'%%Y-%%m') AS date, item.brand, ROUND(SUM(sle.actual_qty)*-1,0) AS sales_qty
        FROM `tabStock Ledger Entry` sle
        JOIN `tabItem` item ON sle.item_code = item.name
        WHERE sle.voucher_type IN ('Sales Invoice','Delivery Note')
            AND sle.is_cancelled = 0
            AND item.item_group = 'Tires'
            AND sle.posting_date > DATE_FORMAT(CURDATE(), '%%Y-01-01')
           {brand_filter}
        GROUP BY date, item.brand
        ORDER BY date
                    """,engine)
    # sle = [dict(row) for row in sle]
    sle=pd.DataFrame(sle)

    # %%
    if sle.empty:
        sales_pivot=pd.DataFrame(columns=['date','sales_qty'])
        years = pd.date_range("2022-01-01", "2024-12-31", freq="YE").strftime('%Y').tolist()
        months = pd.date_range("2025-01-01", "2025-12-01", freq="MS").strftime('%Y-%m').tolist()
        full_dates = years + months
        sales_pivot['date']=full_dates
        sales_pivot.fillna(0,inplace=True)
        sp_pivot=sales_pivot.merge(purchase_pivot,on='date')
        
    else:
        sales_pivot=sle.pivot_table(index=['date'],values=['sales_qty'],aggfunc='sum',fill_value=0,observed=False).reset_index()
        sp_pivot=sales_pivot.merge(purchase_pivot,on='date')

    net_amount=pd.read_sql(f"""
            SELECT item.brand, DATE_FORMAT(si.posting_date,"%%Y") AS date,
            SUM(item.amount - item.discount_amount_custom) AS net_amount
        FROM `tabSales Invoice Item` item
        INNER JOIN `tabSales Invoice` si ON item.parent = si.name
        WHERE item.docstatus = 1
            AND si.docstatus = 1
            AND si.is_debit_note = 0
            AND item.income_account IN (
                SELECT default_income_account FROM `tabCompany`
                UNION
                SELECT default_sales_return_account FROM `tabCompany`
            )
            AND si.posting_date >= DATE_SUB(DATE_FORMAT(CURDATE(), '%%Y-01-01'), INTERVAL 3 YEAR)
            AND si.posting_date < DATE_FORMAT(CURDATE(), '%%Y-01-01')
            {brand_filter}
        GROUP BY item.brand, date

        UNION ALL

        SELECT item.brand, DATE_FORMAT(si.posting_date,"%%Y-%%m") AS date,
            SUM(item.amount - item.discount_amount_custom) AS net_amount
        FROM `tabSales Invoice Item` item
        INNER JOIN `tabSales Invoice` si ON item.parent = si.name
        WHERE item.docstatus = 1
            AND si.docstatus = 1
            AND si.is_debit_note = 0
            AND item.income_account IN (
                SELECT default_income_account FROM `tabCompany`
                UNION
                SELECT default_sales_return_account FROM `tabCompany`
            )
            AND si.posting_date >= DATE_FORMAT(CURDATE(), '%%Y-01-01')
            {brand_filter}
        GROUP BY item.brand, date
            """,engine)
    # net_amount   = [dict(row) for row in net_amount]
    net_amount=pd.DataFrame(net_amount)
    # %%
   
    cogs=pd.read_sql(f""" WITH si_item AS (
            SELECT DISTINCT(si_item.delivery_note) AS delivery_note,
                   si.posting_date AS billing_date
            FROM `tabSales Invoice Item` si_item
            INNER JOIN `tabSales Invoice` si ON si_item.parent = si.name
            WHERE si_item.docstatus = 1
                AND si_item.delivery_note IS NOT NULL
                AND si_item.qty > 0
                AND si_item.income_account IN (
                    SELECT default_income_account FROM `tabCompany`
                )
                AND si.docstatus = 1
                AND si.is_return = 0
                AND si.is_debit_note = 0
        ),
        si_dn AS (
            SELECT name AS sle_id, posting_date AS billing_date FROM `tabSales Invoice`
            WHERE docstatus = 1 AND update_stock = 1
            UNION ALL
            SELECT dn.name AS sle_id,
                   IF(dn.is_return = 1, dn.posting_date, si_item.billing_date) AS billing_date
            FROM `tabDelivery Note` dn
            LEFT JOIN si_item ON dn.name = si_item.delivery_note
            WHERE dn.docstatus = 1
        )
        SELECT item.brand, DATE_FORMAT(si_dn.billing_date,"%%Y") AS date,
               SUM(sle.stock_value_difference) * -1 AS stock_value_difference
        FROM `tabStock Ledger Entry` sle
        INNER JOIN si_dn ON sle.voucher_no = si_dn.sle_id
        INNER JOIN `tabItem` item ON sle.item_code = item.name
        WHERE sle.is_cancelled = 0
            AND sle.voucher_type IN ('Sales Invoice', 'Delivery Note')
            AND si_dn.billing_date IS NOT NULL
            AND si_dn.billing_date >= DATE_SUB(DATE_FORMAT(CURDATE(), '%%Y-01-01'), INTERVAL 3 YEAR)
            AND si_dn.billing_date < DATE_FORMAT(CURDATE(), '%%Y-01-01')
            {brand_filter}
        GROUP BY item.brand, date

        UNION ALL
        
        SELECT item.brand, DATE_FORMAT(si_dn.billing_date,"%%Y-%%m") AS date,
               SUM(sle.stock_value_difference) * -1 AS stock_value_difference
        FROM `tabStock Ledger Entry` sle
        INNER JOIN si_dn ON sle.voucher_no = si_dn.sle_id
        INNER JOIN `tabItem` item ON sle.item_code = item.name
        WHERE sle.is_cancelled = 0
            AND sle.voucher_type IN ('Sales Invoice', 'Delivery Note')
            AND si_dn.billing_date IS NOT NULL
            AND si_dn.billing_date >= DATE_FORMAT(CURDATE(), '%%Y-01-01')
            {brand_filter}
        GROUP BY item.brand, date
            """,engine)
    # cogs   = [dict(row) for row in cogs]
    cogs=pd.DataFrame(cogs)
    if "date" not in cogs.columns:
        cogs["date"] = pd.NaT  # Create an empty date column

    if "date" not in net_amount.columns:
        net_amount["date"] = pd.NaT
    if "brand" not in cogs.columns:
        cogs["brand"] = ""

    if "brand" not in net_amount.columns:
        net_amount["brand"] = ""

    profit_df=cogs.merge(net_amount,how='left',on=['date','brand'])
    

    if profit_df.empty:
        profit_pivot=pd.DataFrame(columns=['date','profitability'])
        years = pd.date_range("2022-01-01", "2024-12-31", freq="YE").strftime('%Y').tolist()
        months = pd.date_range("2025-01-01", "2025-12-01", freq="MS").strftime('%Y-%m').tolist()
        full_dates = years + months
        profit_pivot['date']=full_dates
        profit_pivot.fillna(0,inplace=True)
    else:
        profit_pivot=profit_df.pivot_table(index=['date'],values=['stock_value_difference','net_amount'],aggfunc='sum',fill_value=0).round(1).reset_index()
        profit_pivot['profitability']=(profit_pivot['net_amount']-profit_pivot['stock_value_difference'])/profit_pivot['net_amount']
        profit_pivot=profit_pivot[['date','profitability']]
    
    
    final_pivot=profit_pivot.merge(sp_pivot,how='left',on='date')
    
    years = pd.date_range("2022-01-01", "2024-12-31", freq="YE").strftime('%Y').tolist()
    months = pd.date_range("2025-01-01", "2025-12-01", freq="MS").strftime('%Y-%m').tolist()
    full_dates = years + months
    df=pd.DataFrame()
    df['date']=full_dates
    df=df.merge(final_pivot,how='left',on='date').fillna(0)
    df['AVG_cost']=df['AVG_cost'].round(3)
    
    df['profitability']=df['profitability']*100
    df['profitability']=df['profitability'].apply(lambda x: "{:.2f}%".format(x))
    

    return df.to_dict("records")