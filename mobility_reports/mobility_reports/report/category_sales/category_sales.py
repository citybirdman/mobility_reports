import frappe
import pandas as pd # type: ignore
import requests # type: ignore
from sqlalchemy import create_engine # type: ignore
def execute(filters=None):
	data = get_data(filters)
	columns = get_columns(data)
	return columns, data

def get_columns(data):
    
    base_columns = [
        {"fieldname": "date", "label": "Date", "fieldtype": "Data", "width": 100},
    ]

    if data:
        # Extract brand names dynamically
        brands = {key for row in data for key in row.keys() if key != "date"}

        # Append brand-specific columns
        brand_columns = [
            {"fieldname": brand, "label": brand, "fieldtype": "Int", "width": 100}
            for brand in sorted(brands)
        ]

        base_columns.extend(brand_columns)
    
    return base_columns


def get_data(filters):
    brand_filter = ""
    if filters and filters.get("brand"):
        brand = filters["brand"]
        brand_filter = f"AND item.brand ='{brand}' " 
    
    ssl_url = "https://www.dropbox.com/scl/fi/qg6vaczygt2o572cplm8l/n1-ksa.frappe.cloud._arabian.pem?rlkey=a2gqvzfa997rp4az44z7uftq0&dl=1"
    response = requests.get(ssl_url)

    cert_path = "n1-ksa.frappe.cloud._arabian.pem"
    with open(cert_path, "wb") as f:
        f.write(response.content)

    ssl_args = {"ssl": {"ca": cert_path}}
    connection_string = f"mysql+pymysql://8c48725a15b2a9c:575f0e3b1e05fdb4f4ae@n1-ksa.frappe.cloud:3306/_61c733e77de10d32"
    engine = create_engine(connection_string, connect_args=ssl_args)

    tire_type_sal=pd.read_sql(f'''select DATE_FORMAT(sle.posting_date, '%%Y') as date,item.brand,item.tire_segment,round(sum(sle.actual_qty)*-1,0)as qty from `tabStock Ledger Entry` as sle
                join `tabItem`as item
                on item.name=sle.item_code
                where 
                item.item_group='Tires'
                and sle.is_cancelled=0
                and sle.voucher_type in('Sales Invoice','Delivery Note')
                and sle.posting_date >=DATE_SUB(CURDATE(), INTERVAL 3 YEAR)
                and sle.posting_date < DATE_FORMAT(CURDATE(), '%%Y-01-01')
                {brand_filter}
                group by item.brand,date,item.tire_segment
                
                union all
                
                select DATE_FORMAT(sle.posting_date,'%%Y-%%m') as date,item.brand,item.tire_segment,round(sum(sle.actual_qty)*-1,0)as qty from `tabStock Ledger Entry` as sle
                join `tabItem`as item
                on item.name=sle.item_code
                where 
                item.item_group='Tires'
                and sle.is_cancelled=0
                and sle.voucher_type in('Sales Invoice','Delivery Note')
                and sle.posting_date >= DATE_FORMAT(CURDATE(), '%%Y-01-01')
                {brand_filter}
                group by item.brand, date,item.tire_segment
            
            ''',engine)
    tire_type_sal_df=tire_type_sal.pivot_table(index='date',columns='tire_segment',values='qty',aggfunc='sum',fill_value=0,observed=False).reset_index()
    return tire_type_sal_df.to_dict(orient='records')


