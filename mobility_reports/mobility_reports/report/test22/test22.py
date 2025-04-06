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
    base_columns = [
        {"fieldname": "data", "label": "Date", "fieldtype": "Data", "width": 120},
    ]

    months = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]

    month_columns = [
        {"fieldname": month, "label": month, "fieldtype": "Int", "width": 100}
        for month in months
    ]

    base_columns.extend(month_columns)

    if data:
        additional_columns = sorted(set(data[0].keys()).difference({"data"} | set(months)))

        base_columns.extend(
            {"fieldname": col, "label": col.replace("_", " ").title(), "fieldtype": "Int", "width": 100}
            for col in additional_columns
        )
    
    return base_columns

def get_data(filters):
    shipping_dropbox=pd.read_excel('https://www.dropbox.com/scl/fi/1uxf5y3dny8qsyf1p4pw5/ARABIAN-TIRES-2025-Shipping-Report.xlsx?rlkey=gc5e8gcc5wee4svwep9p32e43&dl=1',header=4,usecols=['Order No.','ETD'])
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
    month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                   'July', 'August', 'September', 'October', 'November', 'December']

    """Order Qty"""
    order_q=pd.read_sql("""
    select DATE_FORMAT(po.schedule_date,'%%M')as date,poi.brand,sum(poi.qty)as qty from `tabPurchase Order Item`as poi
    join `tabPurchase Order`as po
    on po.name=poi.parent
    where DATE_FORMAT(po.schedule_date,"%%Y")=DATE_FORMAT(CURDATE(),"%%Y")
    GROUP BY date,poi.brand""",engine)

    order_q['data']='order_q'

    """Shipping qty"""
    ship_q=pd.read_sql("""
    select pn.title, DATE_FORMAT(pn.shipping_date,"%%Y-%%M")as date,pni.brand,sum(pni.qty)as qty from `tabPurchase Invoice Item`as pni
    join `tabPurchase Invoice`as pn
    on pn.name = pni.parent
    group by pn.title,date,pni.brand""",engine)

    # %%
    ship_df=ship_q.merge(shipping_dropbox,how='left', on='title')

    # %%
    ship_df=ship_df[~ship_df.ETD.isna()].reset_index(drop=True)

    # %%
    ship_df['date']=pd.to_datetime(ship_df['ETD']).dt.month_name()

    # %%
    ship_df=ship_df[['date','qty','brand']].reset_index(drop=True)

    # %%
    ship_df['data']='shipping_q'

    """sales qty"""
    sales_q=pd.read_sql("""
    select DATE_FORMAT(sle.posting_date,'%%M')as date,item.brand, sum(actual_qty)*-1 as qty from `tabStock Ledger Entry`as sle
    join `tabItem` item on item.name=sle.item_code
    where sle.voucher_type in('Sales Invoice','Delivery Note')
    and sle.is_cancelled=0
    and item.item_group='Tires'
    and DATE_FORMAT(sle.posting_date,"%%Y")=DATE_FORMAT(CURDATE(),"%%Y")
    GROUP BY date,item.brand """,engine)

    # %%
    sales_q['data']='sales_q'

    # %%
    """inventory qty"""
    inv_q=pd.read_sql('''
    select item.brand, sle.posting_date as date, sum(actual_qty) as qty from `tabStock Ledger Entry`as sle
    join `tabItem` item on item.name=sle.item_code
    where item.item_group='Tires'
    and sle.is_cancelled=0
    group by item.brand,date
    order by sle.posting_date,item.brand
    ''',engine)

    # %%
    inv_q['year'] = pd.to_datetime(inv_q['date']).dt.strftime('%Y')
    inv_q['month_name'] = pd.to_datetime(inv_q['date']).dt.month_name()

    inv_q['qty'] = inv_q.groupby('brand')['qty'].cumsum()

    # %%
    data_list=['01-31','02-28','03-31','04-30','05-31','06-30','07-31','08-31','09-30','10-31','11-30','12-31']

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

    # %%
    stock['data']='inventory_q'
    def get_zero_filled_table():
        return pd.DataFrame({
        "data": ["order_q", "shipping_q", "sales_q", "inventory_q"],
        **{month: [0] * 4 for month in month_order}
         })  
    # %%
    df=pd.concat([order_q,ship_df,sales_q,stock])

    # %%
    df['date'] = pd.Categorical(df['date'], categories=month_order, ordered=True)
    df = df.sort_values('date')
    if filters and "brand" in df.columns and filters.get("brand"):
        print("Filtering by brand:", filters["brand"])
        df = df[df["brand"] == filters["brand"]]
        if df.empty:
            print("No data found for the given brand, returning zero-filled table")
            pivot_df = get_zero_filled_table()
            
        else:
            pivot_df = df.pivot_table(index="data", columns="date", aggfunc="sum", values="qty", fill_value=0,observed=False)
            pivot_df["data"] = pivot_df.index
            pivot_df["Total"] = pivot_df[month_order].sum(axis=1)
            for month in month_order:
                if month not in pivot_df.columns:
                    pivot_df[month] = 0 
            pivot_df = pivot_df[["data"] + month_order]  
            pivot_df["Total"] = pivot_df[month_order].sum(axis=1)

    return pivot_df.to_dict("records")