import frappe
import pandas as pd # type: ignore
from datetime import datetime

def execute(filters=None):
    data = get_data(filters)
    columns = get_columns(data)
    return columns, data

def get_columns(data):
    # Define static columns
    base_columns = [
        {"fieldname": "data", "label": "Date", "fieldtype": "Data", "width": 120},
    ]

    months = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]

    month_columns = [
        {"fieldname": month, "label": month, "fieldtype": "data", "width": 100}
        for month in months
    ]

    base_columns.extend(month_columns)

    if data:
        additional_columns = sorted(set(data[0].keys()).difference({"data"} | set(months)))

        base_columns.extend(
            {"fieldname": col, "label": col.replace("_", " ").title(), "fieldtype": "data", "width": 100}
            for col in additional_columns
        )
    
    return base_columns

def get_data(filters):
    brand_filter = ""
    if filters and filters.get("brand"):
        brand = filters["brand"]
    
        brand_filter = f"AND item.brand ='{brand}' " 
        
    link=frappe.db.sql(''' select shipping_report_dropbox_shared_uri_path from `tabCompany`''',as_dict=True)
    link = [dict(row) for row in link]

    link = pd.DataFrame(link)
    
    
    shipping_dropbox=pd.read_excel(f'https://www.dropbox.com{link[link.columns[0]][0]}',header=4,usecols=['Order No.','ETD'])
    shipping_dropbox=shipping_dropbox[~shipping_dropbox['ETD'].isna()]
    shipping_dropbox.rename(columns={'Order No.':'title'},inplace=True)
    
    month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                   'July', 'August', 'September', 'October', 'November', 'December']
    

    """Order Qty"""
    order_q=frappe.db.sql(f"""
    select DATE_FORMAT(po.schedule_date,'%%M')as date,item.brand,sum(item.qty)as qty from `tabPurchase Order Item`as item
    join `tabPurchase Order`as po
    on po.name=item.parent
    where DATE_FORMAT(po.schedule_date,"%%Y")=DATE_FORMAT(CURDATE(),"%%Y")
    {brand_filter}
    GROUP BY date,item.brand""",as_dict=True)
    order_q = [dict(row) for row in order_q]

    order_q = pd.DataFrame(order_q)
    if order_q.empty:
        order_q=pd.DataFrame({'date':['0'],'qty':[0],'brand':[0],'data':['Order QTY']})
    else:
        order_q['data']='Order QTY'

    """Shipping qty"""
    ship_q=frappe.db.sql(f"""
    SELECT 
        pn.title, 
        DATE_FORMAT(pn.shipping_date, '%%Y-%%M') AS date, 
        item.brand, 
        SUM(item.qty) AS qty 
    FROM `tabPurchase Invoice Item` AS item
    JOIN `tabPurchase Invoice` AS pn
    ON pn.name = item.parent  -- Fixed alias issue
    where 1=1 {brand_filter}
    GROUP BY pn.title, date, item.brand""",as_dict=True)
    ship_q = [dict(row) for row in ship_q]

    ship_q = pd.DataFrame(ship_q)
    
    if ship_q.empty:
        ship_df=pd.DataFrame({'title':['0'],'date':['0'],'qty':[0],'brand':[0],'data':['Shipping QTY']})
        
    else:
        ship_df=ship_q.merge(shipping_dropbox,how='left', on='title')
        ship_df=ship_df[~ship_df.ETD.isna()].reset_index(drop=True)
        ship_df['date']=pd.to_datetime(ship_df['ETD'],errors='coerce').dt.month_name()
        ship_df=ship_df[['date','qty','brand']].reset_index(drop=True)
        ship_df['data']='Shipping QTY'

    """sales qty"""
    sales_q=frappe.db.sql(f"""
    select DATE_FORMAT(sle.posting_date,'%%M')as date,item.brand, sum(actual_qty)*-1 as qty from `tabStock Ledger Entry`as sle
    join `tabItem` item on item.name=sle.item_code
    where sle.voucher_type in('Sales Invoice','Delivery Note')
    and sle.is_cancelled=0
    and item.item_group='Tires'
    and DATE_FORMAT(sle.posting_date,"%%Y")=DATE_FORMAT(CURDATE(),"%%Y")
    {brand_filter}
    GROUP BY date,item.brand """,as_dict=True)
    sales_q = [dict(row) for row in sales_q]

    sales_q = pd.DataFrame(sales_q)
  
    if sales_q.empty:
        sales_q=pd.DataFrame({'date':['0'],'qty':[0],'brand':[0],'data':['Sales QTY']})
    else:
        sales_q['data']='Sales QTY'    

    """inventory qty"""
    inv_q=frappe.db.sql(f'''
    select item.brand, sle.posting_date as date, sum(actual_qty) as qty from `tabStock Ledger Entry`as sle
    join `tabItem` item on item.name=sle.item_code
    where item.item_group='Tires'
    and sle.is_cancelled=0
    {brand_filter}
    group by item.brand,date
    order by sle.posting_date,item.brand
    ''',as_dict=True)
    inv_q = [dict(row) for row in inv_q]

    inv_q = pd.DataFrame(inv_q)
    
    if inv_q.empty:
        inv_q=pd.DataFrame({'date':['2025-01-01'],'qty':[0],'brand':[0],'data':['Inventory QTY']})
    else:
        inv_q['data']='Inventory QTY'

    
    purchase_order_25=frappe.db.sql(f"""
    SELECT min(po.schedule_date) AS date,pod.item_code, item.brand
    FROM `tabPurchase Order` po
    JOIN `tabPurchase Order Item` pod ON po.name = pod.parent
    JOIN `tabItem` item ON item.name = pod.item_code
    WHERE 
    item.item_group = 'Tires'
    and item.weight_per_unit>0 
    {brand_filter}
    
    group by item.brand,pod.item_code
                """,as_dict=True)
    purchase_order_25 = [dict(row) for row in purchase_order_25]

    purchase_order_25 = pd.DataFrame(purchase_order_25)

    if purchase_order_25.empty:
        purchase_order_25['date'] = pd.to_datetime('2025-01-01')
        purchase_order_25['brand']='0'

    purchase_order_25['date'] = pd.to_datetime(purchase_order_25['date'])

    purchase_order_25=purchase_order_25[purchase_order_25['date'] >= pd.to_datetime('2025-01-01')]
    purchase_order_25.date=pd.to_datetime(purchase_order_25.date).dt.month_name()
    purchase_order_25=purchase_order_25.sort_values(by=['date'])
    
    purchase_order_25['date'] = pd.Categorical(purchase_order_25['date'], categories=month_order, ordered=True)
    purchase_order_25 = purchase_order_25.sort_values('date')

    purchase_order_25=purchase_order_25.pivot_table(columns= 'date',values='brand',aggfunc='count',observed=False)

    purchase_order_25['data']='NEW_SKUs'

    purchase_order_25.reset_index(drop=True,inplace=True)
    purchase_order_25["Total"] = purchase_order_25[month_order].sum(axis=1)  

    net_amount_25=frappe.db.sql(f""" SELECT
                item.brand,
                DATE_FORMAT(si.posting_date,"%%M")AS date,
                SUM(item.amount - item.discount_amount_custom) AS net_amount
            FROM
                `tabSales Invoice Item` item
            INNER JOIN
                `tabSales Invoice` si
            ON
                item.parent = si.name
            WHERE
                item.docstatus = 1
                AND si.docstatus = 1
                AND si.is_debit_note = 0
                AND item.income_account IN (
                    SELECT
                        default_income_account
                    FROM
                        `tabCompany`
                    UNION
                    SELECT
                        default_sales_return_account
                    FROM
                        `tabCompany`
                )
            and si.posting_date>= DATE_FORMAT(CURDATE(), '%%Y-01-01')
            {brand_filter}
            GROUP BY
                item.brand,
                date
    """,as_dict=True)
    net_amount_25 = [dict(row) for row in net_amount_25]

    net_amount_25 = pd.DataFrame(net_amount_25)
    if net_amount_25.empty:
        net_amount_25=pd.DataFrame({'date':['2025-01-01'],'brand':[None],'net_amount':[0]})
    else:
        pass
    
    cogs_25=frappe.db.sql(f"""WITH
    si_item AS (
        SELECT
            DISTINCT(si_item.delivery_note) AS delivery_note,
            si.posting_date AS billing_date
        FROM
            `tabSales Invoice Item` si_item
        INNER JOIN
            `tabSales Invoice` si
        ON
            si_item.parent = si.name
        WHERE
            si_item.docstatus = 1
        AND
            si_item.delivery_note IS NOT NULL
        AND
            si_item.qty > 0
        AND
            si_item.income_account IN (
            SELECT
                default_income_account
            FROM
                `tabCompany`
            )
        AND
            si.docstatus = 1
        AND
            si.is_return = 0
        AND
            si.is_debit_note = 0
    ),
    si_dn AS (
        SELECT
            name AS sle_id,
            posting_date AS billing_date
        FROM
            `tabSales Invoice`
        WHERE
            docstatus = 1
        AND
            update_stock = 1
        UNION ALL
        SELECT
            dn.name AS sle_id,
            IF(dn.is_return = 1, dn.posting_date, si_item.billing_date) AS billing_date
        FROM
            `tabDelivery Note` dn
        LEFT JOIN
            si_item
        ON
            dn.name = si_item.delivery_note
        WHERE
            dn.docstatus = 1
    )
    SELECT
        item.brand,
        DATE_FORMAT(si_dn.billing_date,"%%M")as date,
        SUM(sle.stock_value_difference) * -1 AS stock_value_difference
    FROM
        `tabStock Ledger Entry` sle
    INNER JOIN
        si_dn
    ON
        sle.voucher_no = si_dn.sle_id
    INNER JOIN
        `tabItem` item
    ON
        sle.item_code = item.name
    WHERE
        sle.is_cancelled = 0
    AND
        sle.voucher_type IN ('Sales Invoice', 'Delivery Note')
    AND
        si_dn.billing_date IS NOT NULL
    AND
    si_dn.billing_date>= DATE_FORMAT(CURDATE(), '%%Y-01-01')
    {brand_filter}
    GROUP BY
        item.brand,
        date
            """,as_dict=True)
    cogs_25 = [dict(row) for row in cogs_25]

    cogs_25 = pd.DataFrame(cogs_25)
    
    if cogs_25.empty:
        cogs_25=pd.DataFrame({'date':['2025-01-01'],'brand':[None],'stock_value_difference':[0]})
    else :
        pass

    profit_df_25=cogs_25.merge(net_amount_25,how='left',on=['date','brand'])

    if profit_df_25.empty:
        profit_pivot_25=pd.DataFrame({'total':[0],'data':'GP%','date':'0'})
        profit_pivot_25['date'] = pd.Categorical(profit_pivot_25['date'], categories=month_order, ordered=True)
        profit_pivot_25=profit_pivot_25.pivot_table(columns='date',observed=True)
    else:
        profit_pivot_25=profit_df_25.pivot_table(index=['date'],values=['stock_value_difference','net_amount'],aggfunc='sum',fill_value=0,observed=True).round(1).reset_index()
        
        profit_pivot_25['profitability']=(profit_pivot_25['net_amount']-profit_pivot_25['stock_value_difference'])/profit_pivot_25['net_amount']


        profit_pivot_25['date'] = pd.Categorical(profit_pivot_25['date'], categories=month_order, ordered=True)
        profit_pivot_25=profit_pivot_25.pivot_table(columns='date',values='profitability',observed=True)

        profit_pivot_25['data']='GP%'

        profit_pivot_25.columns[:-2]

        profit_pivot_25["Total"] = profit_pivot_25[profit_pivot_25.columns[:-2]].mean(axis=1) 
        for col in [c for c in month_order + ['Total'] if c in profit_pivot_25.columns]:
            profit_pivot_25[col] = profit_pivot_25[col].apply(lambda x: f"{x * 100:.2f}%" if pd.notnull(x) else "0.00%")


    inv_q['year'] = pd.to_datetime(inv_q['date']).dt.strftime('%Y')
    inv_q['month_name'] = pd.to_datetime(inv_q['date']).dt.month_name()

    inv_q['qty'] = inv_q.groupby('brand')['qty'].cumsum()

    data_list=['01-31','02-28','03-31','04-30','05-31','06-30','07-31','08-31','09-30','10-31','11-30','12-31']

    inv_q['date']=pd.to_datetime(inv_q['date']).dt.date
    
    dfs=[]
    for i in data_list:
        if pd.to_datetime(f'{datetime.now().year}-{i}').month<=datetime.now().month:
            
            date=pd.to_datetime(f'{datetime.now().year}-{i}').date()
            
            x=inv_q [inv_q['date']<= (date)]
            stock = x.loc[x.groupby('brand')['date'].idxmax(), ['brand', 'year','month_name', 'qty']].reset_index(drop=True)
            stock['year']=datetime.now().year
            stock['date']=pd.to_datetime(f'{datetime.now().year}-{i}').month_name()
            dfs.append(stock)
    stock=pd.concat(dfs).reset_index(drop=True)
    
    stock['data']='Inventory QTY'

    df=pd.concat([order_q,ship_df,sales_q,stock]).fillna(0)
    df['date'] = pd.Categorical(df['date'], categories=month_order, ordered=True)
    df = df.sort_values('date')

    inv_df=df.pivot_table(index='data' ,columns='date',aggfunc='sum', values='qty',fill_value=0,observed=False)

    inv_df['data']=inv_df.index

    inv_df["Total"] = inv_df[month_order].sum(axis=1)

    inv_df.reset_index(drop=True,inplace=True)

    final_df=pd.concat([inv_df,purchase_order_25,profit_pivot_25]).fillna(0).reset_index(drop=True)

    final_df=final_df.round(0)

    return final_df.to_dict("records")
