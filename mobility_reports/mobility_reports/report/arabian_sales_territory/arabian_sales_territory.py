import frappe
import pandas as pd # type: ignore
import numpy as np # type: ignore


def execute(filters=None):

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

    # Load data into Pandas DataFrame
    stock_df = frappe.db.sql(sql_stock, as_dict=True)
    stock_df=pd.DataFrame([dict(row) for row in stock_df])
    
    if stock_df.empty:
        frappe.logger().info("Empty stock_df: No data available.")
        return []

    # Pivot Tables
    region_details = stock_df.pivot_table(index=['date', 'customer', 'parent_territory', 'brand'],
                                          values='actual_qty', aggfunc='sum', fill_value=0).reset_index()

    region_origin = stock_df.pivot_table(index=['date', 'parent_territory'],
                                         columns=['country_of_origin'], values='actual_qty',
                                         aggfunc='sum', fill_value=0).reset_index()

    city_origin = stock_df.pivot_table(index=['date', 'territory'],
                                       columns=['country_of_origin'], values='actual_qty',
                                       aggfunc='sum', fill_value=0).reset_index()

    # Normalize percentages
    total_qty = region_origin.set_index(['date', 'parent_territory']).sum(axis=1)
    total_qty.replace(0, np.nan, inplace=True)
    region_origin_percentage = region_origin.set_index(['date', 'parent_territory']).div(total_qty, axis=0) * 100
    region_origin_percentage.fillna(0, inplace=True)
    region_origin_percentage.reset_index(inplace=True)

    total_qty_city = city_origin.set_index(['date', 'territory']).sum(axis=1)
    total_qty_city.replace(0, np.nan, inplace=True)
    city_origin_percentage = city_origin.set_index(['date', 'territory']).div(total_qty_city, axis=0) * 100
    city_origin_percentage.fillna(0, inplace=True)
    city_origin_percentage.reset_index(inplace=True)

    # Ensure all tables are valid
    if region_details.empty or region_origin_percentage.empty or city_origin_percentage.empty:
        frappe.logger().info("Pivot tables are empty.")
        return []

    def df_to_list(df):
        """Convert DataFrame to Frappe report format."""
        return df.columns.tolist(), df.values.tolist()

    report_output = [
        {
            "label": "Region Details",
            "columns": df_to_list(region_details)[0],
            "data": df_to_list(region_details)[1]
        },
        {
            "label": "Region Origin Percentage",
            "columns": df_to_list(region_origin_percentage)[0],
            "data": df_to_list(region_origin_percentage)[1]
        },
        {
            "label": "City Origin Percentage",
            "columns": df_to_list(city_origin_percentage)[0],
            "data": df_to_list(city_origin_percentage)[1]
        }
    ]

    frappe.logger().info(f"Report Output: {report_output}")  # Debugging log
    return report_output
