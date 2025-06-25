import pandas as pd # type: ignore
from datetime import datetime
now=datetime.now()
import frappe


def execute(filters=None):
    data=get_data(filters)
    columns=get_columns(data)
    return columns, data



def get_columns(data):
    if not data:
        return []

    first_row = data[0]
    return [
        {
            "label": key.replace("_", " ").title(),
            "fieldname": key,
            "fieldtype": "Data",
            "width": 150
        }
        for key in first_row.keys()
    ]


def get_data(filters):
    month=filters.get('month')
    value_type=filters.get('value_type')
    agg_type=filters.get('aggregation_type')
    print(month, value_type, agg_type)
    if value_type=='Qty':
        value_type='qty'
    else:
        value_type='base_amount'
    if agg_type=='Customer':
        agg_type='customer_name'
    elif agg_type=='Category':
        agg_type='catogory'
    elif agg_type=='Location Zone':
        agg_type='custom_location_zone'
    elif agg_type=='Sales Channel':
        agg_type='sales_channel'
    elif agg_type=='SKU':
        agg_type='SKU'
        
  
    omg_24=pd.read_csv("https://www.dropbox.com/scl/fi/yvz5bm17177jdu0ok4z3v/OMG-Sales-24-2024-Sales.csv?rlkey=fe05jv6pp86h4ma6aehe6w8os&st=x1cgkd35&dl=1")
    
    omg_24.posting_date=pd.to_datetime(omg_24.posting_date, format='mixed', dayfirst=True)
    omg_24['catogory']=omg_24.item_code.str.split('-').str[0].str.strip().str.title()
    omg_24['flover'] = omg_24.item_code.str.split('-').str[1].str.strip().str.title()
    
    omg_25 = frappe.db.sql(
    """ 
    SELECT 
        sii.name,
        si.name AS sales_invoice_id,
        si.customer,
        c.customer_name,
        c.custom_location_zone,
        c.territory,
        c.custom_source,
        sii.warehouse,
        sii.item_code,
        sii.item_name,
        si.posting_date,
        si.custom_invoice_number,
        si.custom_shipping_cost,
        c.custom_status,
        si.custom_order_method,
        si.custom_sales_chanal AS custom_Sales_channel,
        si.custom_payment_status,
        MAX(sii.conversion_factor) AS conversion_factor,
        MAX(si.custom_code_amount) AS base_discount_amount,
        MAX(si.custom_additional_discount_amount1) AS discount_amount,
        SUM(sii.qty) AS qty,
        MAX(sii.base_rate) AS base_rate,
        SUM(sii.base_net_amount) AS base_amount,
        (MAX(si.custom_vat) +max( si.custom_shipping_cost))+ SUM(sii.base_amount) AS amount_after_vat
    FROM `tabSales Invoice` si
    JOIN `tabSales Invoice Item` sii 
        ON si.name = sii.parent 
    join `tabCustomer` c 
        on si.customer = c.name
    WHERE si.docstatus = 1 
      AND si.status <> 'Cancelled'
      and si.docstatus = 1
    GROUP BY 
        sii.name,
        si.name,
        si.customer,
        si.customer_name,
        si.custom_location_zone,
        si.territory,
        sii.warehouse,
        sii.item_code,
        sii.item_name,
        si.posting_date,
        si.custom_invoice_number,
        si.custom_shipping_cost,
        c.custom_status,
        si.custom_order_method,
        si.custom_sales_chanal,
        si.custom_payment_status
    """,
    as_dict=True,
)	
    omg_25=pd.DataFrame([dict(row) for row in omg_25])
    df=pd.concat([omg_24, omg_25]).reset_index(drop=True)
    df['posting_date'] = pd.to_datetime(df['posting_date'], format='mixed', dayfirst=True).dt.strftime('%Y-%m-%d')
    df.territory=df.territory.str.strip().str.upper()
    df.custom_location_zone=df.custom_location_zone.str.strip().str.upper()
    df.custom_Sales_channel=df.custom_Sales_channel.str.strip().str.title()
    df.custom_invoice_number=df.custom_invoice_number.str.strip().str.title()
    df.custom_status=df.custom_status.str.strip().str.title()
    df.custom_order_method=df.custom_order_method.str.strip().str.title()
    df.customer_name=df.customer_name.str.strip().str.title()
    df.item_code=df.item_code.str.strip().str.title()
    df.item_name=df.item_name.str.strip().str.title()
    df.custom_payment_status=df.custom_payment_status.str.strip().str.title()
    df['catogory']=df.item_code.str.split('-').str[0].str.strip().str.title()
    df['flavor'] = df.item_code.str.split('-').str[1].str.strip().str.title()
    df['year']=pd.to_datetime(df.posting_date).dt.year
    df['month_name']=pd.to_datetime(df.posting_date).dt.month_name().str.title()
    df.rename(columns={'flavor': 'SKU', 'custom_Sales_channel': 'sales_channel'}, inplace=True)
    df=df[df['year'].isin([now.year-1, now.year])]
    df['custom_column']=df['year'].astype(str)+' ('+df['month_name']+')' 
    x=df[df.month_name==f'{month}'].reset_index(drop=True)
    df=x.pivot_table(index=f'{agg_type}', columns='custom_column', values=f'{value_type}', aggfunc='sum',fill_value=0).reset_index()
    
    return df.to_dict(orient='records')