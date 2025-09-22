import frappe
import pandas as pd # type: ignore


def execute(filters=None):
	data = get_data(filters)
	columns=get_column(data)
	return columns, data


def get_column(data):
    base_columns = [
        {"fieldname": "name", "label": "name", "fieldtype": "Data", "width": 100},
        {"fieldname": "brand", "label": "brand", "fieldtype": "Data", "width": 100},
        {"fieldname": "tire_size", "label": "tire_size", "fieldtype": "Data", "width": 100},
        {"fieldname": "ply_rating", "label": "ply_rating", "fieldtype": "Data", "width": 100},
        {"fieldname": "country_of_origin", "label": "country_of_origin", "fieldtype": "Data", "width": 100},
        {"fieldname": "price", "label": "Company Price", "fieldtype": "Data", "width": 100},
    ]
    if data:
        # Extract brand names dynamically
        brands = {key for row in data for key in row.keys() if key not in('country_of_origin','tire_size','ply_rating','brand','name','Arabian Price')}

        # Append brand-specific columns
        brand_columns = [
            {"fieldname": brand, "label": brand, "fieldtype": "int", "width": 300}
            for brand in brands
        ]

        base_columns.extend(brand_columns)
    return base_columns

def get_data(filters):
    price_list=filters.get("price_list")
	from_date_filter = "1=1"
	if filters and filters.get("from_date"):
		from_date = filters["from_date"]
		from_date_filter = f" creation >= '{from_date}'"
  
	
	production_year_filter='and 1=1'
	if filters and filters.get("production_year"):
		from_year = filters["production_year"]
		production_year_filter=f" and production_year='{from_year}'"
	brand_filter='and 1=1'
	if filters and filters.get("brand"):
		from_brand = filters["brand"]
		brand_filter=f" and item.brand='{from_brand}'"
     
	item_price=frappe.db.sql(f"""
			select item.name, item.item_name,
			item.brand,
			item.country_of_origin,
			item.tire_size, 
			item.ply_rating,
			ip.production_year,
			ip.valid_from,
			ip.price_list_rate as `price`
			from `tabItem`as item 
			join `tabItem Price` as ip on item.name = ip.item_code
			where item_group = "Tires"
			and ip.price_list='{price_list}'
			{production_year_filter}
			{brand_filter}
                       """,as_dict=True)
	item_price   = [dict(row) for row in item_price]
	item_price= pd.DataFrame(item_price)
 
	market_price=frappe.db.sql(f"""
				select market_brand,
				creation,
				origin as country_of_origin,
				distributor,
				tire_size,
				pr as ply_rating,
				price,
				production_year
				from `tabItem Market Price` 
    			where {from_date_filter} 
       			{production_year_filter}
                         """,as_dict=True)
	market_price   = [dict(row) for row in market_price]
	market_price=pd.DataFrame(market_price)
 
	market_price=market_price.sort_values(['creation'])
	
	df=item_price.merge(market_price,how='left',on=['tire_size','ply_rating','country_of_origin']).fillna('0')
 
	df['distributor/brand']=df['distributor']+' / '+df['market_brand']
 
	pivot_df=df.pivot_table(index=['name','brand','tire_size','ply_rating','country_of_origin','Arabian Price']\
                         ,columns=['distributor/brand'],values='price',aggfunc='last',observed=True).reset_index().infer_objects(copy=False).fillna(0)
 
	if '0 / 0' in pivot_df.columns:
		pivot_df.drop(columns=['0 / 0'], inplace=True)
	
	return pivot_df.to_dict(orient='records')
    