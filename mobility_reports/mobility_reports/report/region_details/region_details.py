import frappe
import pandas as pd # type: ignore
from sqlalchemy import create_engine # type: ignore
import requests # type: ignore

def execute(filters=None):
    data = get_data()
    columns= get_columns(data)
    return columns, data

def get_columns(data):
    base_columns = [
        {"fieldname": "date", "label": "Year", "fieldtype": "Data", "width": 200},
        {"fieldname": "customer", "label": "Customer", "fieldtype": "Data", "width": 200},
        {"fieldname": "parent_territory", "label": "Territory", "fieldtype": "Data", "width": 200},
        {"fieldname": "brand", "label": "Brand", "fieldtype": "Data", "width": 200},
        {"fieldname": "actual_qty", "label": "QTY", "fieldtype": "Data", "width": 200},    
    ]
    
    return base_columns
def get_data():
    ssl_url = "https://www.dropbox.com/scl/fi/qg6vaczygt2o572cplm8l/n1-ksa.frappe.cloud._arabian.pem?rlkey=a2gqvzfa997rp4az44z7uftq0&dl=1"
    response = requests.get(ssl_url)
    cert_path = "n1-ksa.frappe.cloud._arabian.pem"
    with open(cert_path, "wb") as f:
        f.write(response.content)
    ssl_args = {"ssl": {"ca": cert_path}}
    connection_string = f"mysql+pymysql://8c48725a15b2a9c:575f0e3b1e05fdb4f4ae@n1-ksa.frappe.cloud:3306/_61c733e77de10d32"
    engine = create_engine(connection_string, connect_args=ssl_args)

    sql_stock = """ WITH customer_table AS (
							SELECT 
								c.name, 
								c.customer_name, 
								c.customer_group, 
								c.territory, 
								cg.branch 
							FROM tabCustomer c
							JOIN `tabCustomer Group` cg ON c.customer_group = cg.name
						),
						new_table AS (
							SELECT 
								s.name, 
								s.posting_date, 
								s.customer, 
								c.territory, 
								c.branch 
							FROM `tabSales Invoice` s
							JOIN customer_table c ON c.name = s.customer
							WHERE s.docstatus = 1 
							#   AND s.income_account IN ("4122 - مردودات المبيعات - ARA", "411 - المبيعات - ARA") 
							#   AND s.is_opening = 0 
							#   AND s.is_debit_note = 0
							UNION ALL
							SELECT 
								d.name, 
								d.posting_date, 
								d.customer, 
								c2.territory, 
								c2.branch 
							FROM `tabDelivery Note` d
							JOIN customer_table c2 ON c2.name = d.customer
							WHERE d.docstatus = 1
						)

						SELECT 
							s.item_code,
							DATE_FORMAT(s.posting_date,"%%Y")date,
							s.voucher_type,
							s.voucher_no,
							SUM(s.actual_qty)*-1  AS actual_qty,
							n.territory,
							n.branch,
							n.customer,
							item.item_group,
							item.brand,
							item.country_of_origin,
							item.tire_size,
							item.ply_rating,
							te.parent_territory 
						FROM `tabStock Ledger Entry` s
						JOIN new_table n ON n.name = s.voucher_no
						JOIN tabItem item ON item.name = s.item_code 
						JOIN tabTerritory te ON te.name = n.territory
						WHERE s.is_cancelled = 0 
						AND s.voucher_type IN ('Sales Invoice', 'Delivery Note') and item.item_group='Tires'
						GROUP BY 
							s.item_code,
							date,
							s.voucher_type,
							s.voucher_no,
							n.territory,
							n.branch,
							n.customer,
							item.item_group,
							item.brand,
							item.country_of_origin,
							item.tire_size,
							item.ply_rating,
							te.parent_territory"""

    stock_df = pd.read_sql(sql_stock, engine)

    if stock_df.empty:
        frappe.logger().info("Empty stock_df: No data available.")
        return []

    region_details = stock_df.pivot_table(index=['date', 'customer', 'parent_territory', 'brand'],
                                          values='actual_qty', aggfunc='sum', fill_value=0).reset_index()
    return region_details.to_dict(orient='records')
