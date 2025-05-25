import frappe

def execute(filters=None):
    data = get_data(filters)
    columns = get_columns(data)
    return columns, data

def get_columns(data):
    base_columns = [
        {"fieldname": "customer", "label": "Customer", "fieldtype": "Data", "width": 120},
        {"fieldname": "customer_name", "label": "Customer Name", "fieldtype": "Data", "width": 150},
        {"fieldname": "brand", "label": "Brand", "fieldtype": "Data", "width": 100},
    ]

    if data:
        dynamic_fields = list(data[0].keys())[3:]  # skip fixed columns
        for field in dynamic_fields:
            if field.startswith("qty("):
                fieldtype = "Float"
            elif field.startswith("amount("):
                fieldtype = "Currency"
            else:
                fieldtype = "Data"

            base_columns.append({
                "fieldname": field,
                "label": field,
                "fieldtype": fieldtype,
                "width": 120
            })

    return base_columns

def get_data(filters):
    # Optional: use filters like filters.get("from_date")
    data = frappe.db.sql("""
        SELECT
            si.customer,
            si.customer_name,
            sii.brand,
            DATE_FORMAT(si.posting_date,'%Y-%m') AS date,
            sii.qty,
            sii.amount
        FROM
            `tabSales Invoice Item` sii
        JOIN
            `tabSales Invoice` si ON sii.parent = si.name
        WHERE
            si.docstatus = 1
            AND si.posting_date BETWEEN '2025-01-01' AND '2025-05-15'
    """, as_dict=True)

    dates = sorted(set(row['date'] for row in data))
    pivot_map = {}

    for row in data:
        key = (row['customer'], row['customer_name'], row['brand'])
        date = row['date']

        if key not in pivot_map:
            pivot_map[key] = {}

        if date not in pivot_map[key]:
            pivot_map[key][date] = {'qty': 0, 'amount': 0}

        pivot_map[key][date]['qty'] = pivot_map[key][date]['qty'] + row['qty']
        pivot_map[key][date]['amount'] = pivot_map[key][date]['amount'] + row['amount']

    result = []
    for key, date_data in pivot_map.items():
        customer, customer_name, brand = key
        row = {
            "customer": customer,
            "customer_name": customer_name,
            "brand": brand
        }
        for date in dates:
            row[f"qty({date})"] = date_data.get(date, {}).get("qty", 0)
            row[f"amount({date})"] = date_data.get(date, {}).get("amount", 0)
        result.append(row)

    return result
