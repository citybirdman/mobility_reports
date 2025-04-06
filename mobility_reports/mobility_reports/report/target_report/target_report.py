import frappe
import pandas as pd # type: ignore
import numpy as np # type: ignore
# import requests # type: ignore
# from sqlalchemy import create_engine # type: ignore

def execute(filters=None):
    data = get_data()
    columns = get_columns(data)
    return columns, data

def get_columns(data):
    base_columns = [
        {"fieldname": "country_of_origin", "label": "Origin", "fieldtype": "Data", "width": 100},
        {"fieldname": "tire_size", "label": "Tire Size", "fieldtype": "Data", "width": 100},
        {"fieldname": "ply_rating", "label": "PLY Rating", "fieldtype": "Data", "width": 100},
        {"fieldname": "TOTAL STOCK", "label": "TOTAL STOCK", "fieldtype": "int", "width": 150},
        {"fieldname": "AVG_Sales", "label": "AVG Sales", "fieldtype": "Float", "width": 150},
        {"fieldname": "Months on Hand", "label": "Months on Hand", "fieldtype": "int", "width": 150},
                {"fieldname": "Revised Months on Hand", "label": "Revised Months on Hand", "fieldtype": "Float", "width": 150},
                {"fieldname": "total_target", "label": "Sales Target", "fieldtype": "int", "width": 150},
                {"fieldname": "Actual Sales", "label": "Actual Sales", "fieldtype": "int", "width": 150},
                {"fieldname": "Fulfilled %", "label": "Fulfilled %", "fieldtype": "Percent", "width": 150},
    ]
    target=[]
    sales=[]
    stock=[]
    # months=[]
    if data:
        
        excluded_fields = {
            "country_of_origin", "tire_size", "ply_rating",
            "TOTAL STOCK", "AVG_Sales",
            "Months on Hand", "Revised Months on Hand", "total_target",'Fulfilled %',"Actual Sales"
        }
        brands = {key for row in data for key in row.keys() if key not in excluded_fields}


        for brand in brands:
            
            if 'SALES' in brand :
                brand_columns = [
                {"fieldname": brand, "label": brand, "fieldtype": "int", "width": 300}
                ]
                sales.extend(brand_columns)
            
            
            
            elif 'TARGET'in brand:
                target_columns = [
                {"fieldname": brand, "label": brand, "fieldtype": "int", "width": 300}
                 ]
                target.extend(target_columns)

            elif 'STOCK' in brand :
                stock_columns = [
                    {"fieldname": brand, "label": brand, "fieldtype": "int", "width": 300}         
                    ]
                stock.extend(stock_columns)
            else :
                # months_columns = [
                #     {"fieldname": brand, "label": brand, "fieldtype": "data", "width": 300}         
                #     ]
                # months.extend(months_columns)
                pass
                
    # base_columns.extend(months)
    base_columns.extend(sales)
    base_columns.extend(target)
    base_columns.extend(stock)
    return base_columns


def get_data():

    # ssl_url = "https://www.dropbox.com/scl/fi/qg6vaczygt2o572cplm8l/n1-ksa.frappe.cloud._arabian.pem?rlkey=a2gqvzfa997rp4az44z7uftq0&dl=1"
    # response = requests.get(ssl_url)

    # # Save it as a local file
    # cert_path = "n1-ksa.frappe.cloud._arabian.pem"
    # with open(cert_path, "wb") as f:
    #     f.write(response.content)

    # # %%
    # ssl_args = {"ssl": {"ca": cert_path}}
    # connection_string = f"mysql+pymysql://8c48725a15b2a9c:575f0e3b1e05fdb4f4ae@n1-ksa.frappe.cloud:3306/_61c733e77de10d32"
    # engine = create_engine(connection_string, connect_args=ssl_args)

    # %%
    item_brand=frappe.db.sql("""
    SELECT 
        item.country_of_origin,
        item.tire_size,
        item.ply_rating,
        w.warehouse_name,
        round(SUM(sle.actual_qty),0) AS total_qty
    FROM `tabItem` AS item
    JOIN `tabStock Ledger Entry` AS sle
        ON item.name = sle.item_code
    join
        `tabWarehouse` w
    on
        sle.warehouse = w.name
    WHERE 
        item.item_group = 'Tires' 
        and sle.is_cancelled = 0
        and w.warehouse_type in ('FC', 'DC')
    GROUP BY  
        item.country_of_origin,
        item.tire_size,
        item.ply_rating,
        w.warehouse_name
                """,as_dict=True)
    item_brand = [dict(row) for row in item_brand]

    item_brand = pd.DataFrame(item_brand)
    print(item_brand)
    # %%
    item_brand.warehouse_name=item_brand.warehouse_name.astype('str')+'_STOCK'

    # %%
    item_brand_df=item_brand.pivot_table(index=['country_of_origin','tire_size','ply_rating'],columns=['warehouse_name'],values='total_qty',aggfunc='sum',fill_value=0)

    item_brand_df['TOTAL STOCK'] = item_brand_df.sum(axis=1)

    item_brand_df['ply_rating']=item_brand_df.index.map(lambda x:x[2])
    item_brand_df['tire_size']=item_brand_df.index.map(lambda x:x[1])
    item_brand_df['country_of_origin']=item_brand_df.index.map(lambda x:x[0])
    item_brand_df.reset_index(drop=True,inplace=True)

    # %%
    item_brand_df=item_brand_df[item_brand_df.columns[::-1]]

    # %%
    sales_order=frappe.db.sql("""SELECT 
        item.country_of_origin,
        item.tire_size,
        item.ply_rating,
        DATE_FORMAT(sle.posting_date, '%%Y-%%m') as date,
        round(SUM(sle.actual_qty)*-1,0) AS total_qty
    FROM `tabItem` AS item
    JOIN `tabStock Ledger Entry` AS sle
        ON item.name = sle.item_code

    WHERE 
        item.item_group = 'Tires' 
        and sle.is_cancelled = 0
        and sle.docstatus = 1
        and sle.voucher_type in ('Sales Invoice', 'Delivery Note')
        and sle.posting_date >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
        and sle.posting_date < DATE_FORMAT(CURDATE(), '%%Y-%%m-01') 

    GROUP BY  
        item.country_of_origin,
        item.tire_size,
        item.ply_rating,
        date
    """,as_dict=True)
    sales_order = [dict(row) for row in sales_order]

    sales_order = pd.DataFrame(sales_order)


    # %%
    sales_order_df=sales_order.pivot_table(index=['country_of_origin','tire_size','ply_rating'],columns=['date',],values='total_qty',aggfunc='sum',fill_value=0)

    # %%
    columns_to_average = [col for col in sales_order_df.columns if col.startswith("2024") or col.startswith("2025") or col.startswith("2026") or col.startswith('2027')]
    sales_order_df['AVG_Sales'] = sales_order_df[columns_to_average].mean(axis=1)

    # %%
    sales_order_df['ply_rating']=sales_order_df.index.map(lambda x:x[2])
    sales_order_df['tire_size']=sales_order_df.index.map(lambda x:x[1])
    sales_order_df['country_of_origin']=sales_order_df.index.map(lambda x:x[0])
    sales_order_df.reset_index(drop=True,inplace=True)

    # %%
    sales_order_df=sales_order_df[sales_order_df.columns[::-1]]
   
    
    sales_warehouse=frappe.db.sql("""SELECT 
        item.country_of_origin,
        item.tire_size,
        item.ply_rating,
        w.warehouse_name,
        round(SUM(sle.actual_qty)*-1,0) AS total_qty
    FROM `tabItem` AS item
    JOIN `tabStock Ledger Entry` AS sle
        ON item.name = sle.item_code
    JOIN
        `tabWarehouse` w
    ON
        sle.warehouse = w.name
    WHERE 
        item.item_group = 'Tires' 
        and sle.is_cancelled = 0
        and sle.docstatus = 1
        and sle.voucher_type in ('Sales Invoice', 'Delivery Note')
        and DATE_FORMAT(sle.posting_date, '%%Y-%%m')  = DATE_FORMAT(CURDATE(), '%%Y-%%m') 
        and w.warehouse_type IN ('FC', 'DC')

    GROUP BY  
        item.country_of_origin,
        item.tire_size,
        item.ply_rating,
        w.warehouse_name
        
    """,as_dict=True)
    sales_warehouse = [dict(row) for row in sales_warehouse]

    sales_warehouse = pd.DataFrame(sales_warehouse)

    # %%
    sales_warehouse.warehouse_name=sales_warehouse.warehouse_name.astype('str')+'_SALES'

    # %%
    sales_warehouse_df=sales_warehouse.pivot_table(index=['country_of_origin','tire_size','ply_rating'],columns=['warehouse_name'],values='total_qty',aggfunc='sum',fill_value=0)

    # %%
    sales_warehouse_df['Actual Sales']=sales_warehouse_df.sum(axis=1)

    # %%
    sales_warehouse_df['ply_rating']=sales_warehouse_df.index.map(lambda x:x[2])
    sales_warehouse_df['tire_size']=sales_warehouse_df.index.map(lambda x:x[1])
    sales_warehouse_df['country_of_origin']=sales_warehouse_df.index.map(lambda x:x[0])
    sales_warehouse_df.reset_index(drop=True,inplace=True)

    # %%
    sales_warehouse_df=sales_warehouse_df[sales_warehouse_df.columns[::-1]]


    # %%
    merged_order_warehouse=sales_order_df.merge(sales_warehouse_df,on=['country_of_origin','tire_size','ply_rating'])

    # %%
    item_brand_df_merged=item_brand_df.merge(merged_order_warehouse,on=['country_of_origin','tire_size','ply_rating'])

    # %%
    # item_brand_df_merged.columns=item_brand_df_merged.columns.str.replace('_x','_STOCK').str.replace('_y','_SALES')

    # %%
    item_brand_df_merged['Months on Hand']=item_brand_df_merged['TOTAL STOCK']/item_brand_df_merged['AVG_Sales']

    target=frappe.db.sql("""
    select w.warehouse_name,st.origin,std.ply_rating,std.tire_size ,std.target_qty from `tabSales Target`st
    join `tabSales Target Detail` std on st.name = std.parent
    join `tabWarehouse` w on st.warehouse=w.name
    where DATE_FORMAT(st.month_date,'%%Y-%%M')= DATE_FORMAT(CURDATE(), '%%Y-%%M') 
    """,as_dict=True)
    target = [dict(row) for row in target]

    target = pd.DataFrame(target)
    target.warehouse_name=target.warehouse_name.astype('str')+'_TARGET'
    # %%
    target_df=target.pivot_table(index=['origin','tire_size','ply_rating'],columns=['warehouse_name'],values='target_qty',aggfunc='sum',fill_value=0)

    # %%
    target_df['total_target']=target_df.sum(axis=1)

    # %%
    target_df.columns=target_df.columns.str.replace('-','_TARGET_')

    # %%
    target_df['ply_rating']=target_df.index.map(lambda x:x[2])
    target_df['tire_size']=target_df.index.map(lambda x:x[1])
    target_df['country_of_origin']=target_df.index.map(lambda x:x[0])
    target_df.reset_index(drop=True,inplace=True)

    # %%
    target_df=target_df[target_df.columns[::-1]]

    # %%
    target_df

    # %%
    item_brand_df_merged=item_brand_df_merged.merge(target_df,how='left', on=['country_of_origin','tire_size','ply_rating'])

    # %%
    item_brand_df_merged.sort_values(['total_target'],inplace=True,ascending=False)

    # %%
    item_brand_df_merged.fillna(0,inplace=True)

    # %%
    item_brand_df_merged['Revised Months on Hand']=(item_brand_df_merged['TOTAL STOCK']/item_brand_df_merged['total_target'])

    # %%
    item_brand_df_merged.replace([np.inf, -np.inf, np.nan], 0, inplace=True)

    # %%
    item_brand_df_merged.reset_index(drop=True,inplace=True)

    # %%
    item_brand_df_merged['Fulfilled %']=(item_brand_df_merged['Actual Sales']/item_brand_df_merged['total_target'])

    # %%
    item_brand_df_merged.country_of_origin=item_brand_df_merged.country_of_origin.astype(str)
    item_brand_df_merged.tire_size=item_brand_df_merged.tire_size.astype(str)
    item_brand_df_merged.ply_rating=item_brand_df_merged.ply_rating.astype(str)
    item_brand_df_merged['Fulfilled %']=item_brand_df_merged['Fulfilled %'].replace(float('inf'),0).astype(float)
    
    return item_brand_df_merged.to_dict('records')
