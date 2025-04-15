import pandas as pd # type: ignore
import frappe
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
db_lock = Lock()

@frappe.whitelist()
def data_execute(company, customer):
    def ageing(company, customer):
        filters = {
            "company": company,
            "ageing_based_on": "Posting Date",
            "range": "30, 45, 60, 90",
            "party_type": "Customer",
            "party": [customer]
        }

        result = frappe.call(
            "frappe.desk.query_report.run",
            report_name="Accounts Receivable Summary",
            filters=filters,
            ignore_prepared_report=True
        )

        raw_rows = result.get("result", [])
        data_rows = [row for row in raw_rows if isinstance(row, dict)]

        cleaned_rows = []
        for row in data_rows:
            clean_row = {}
            for key, value in row.items():
                if isinstance(value, (list, dict)):
                    clean_row[key] = None
                else:
                    clean_row[key] = value
            cleaned_rows.append(clean_row)

        ageing = pd.DataFrame(cleaned_rows)
        if ageing.empty:
            ageing=pd.DataFrame({'outstanding':[0],'range1':[0],'range2':[0],'range3':[0],'range4':[0],'range5':[0],})
        buckets = {
            'Outstanding': ageing['outstanding'],
            '>30':  (ageing['range2'] + ageing['range3'] + ageing['range4'] + ageing['range5']).round(1),
            '>45':  (ageing['range3'] + ageing['range4'] + ageing['range5']).round(1),
            '>60':  (ageing['range4'] + ageing['range5']).round(1),
            '>90':  (ageing['range5']).round(1)
            }

        age_df = pd.DataFrame(buckets, index=[0])
        
        return age_df
    ageing_df=ageing(company, customer)
    
    queries=[
        
        f""" 
        SELECT
            c.name AS Customer,
            c.customer_name,
            c.customer_group,
            c.disabled,
            c.payment_terms,
            c.sales_person,
            cg.branch,
            ccl.credit_limit AS internal_credit_limit,
            ccl.legal_credit_limit
        FROM
            `tabCustomer` c
            left join `tabCustomer Group` cg on cg.name = c.customer_group
            left join `tabTerritory` t on t.name = c.territory
            left join `tabCustomer Credit Limit` ccl on ccl.parent = c.name
        WHERE
            c.name = %s""",
            
        f"""
        SELECT
            posting_date,
            SUM(received_amount) AS received_amount
        FROM
            `tabPayment Entry`
        WHERE
            docstatus = 1
            AND party_type = 'Customer'
            AND payment_type = 'Receive'
            and party=%s
            AND DATEDIFF(CURDATE(), posting_date) <= 720
        GROUP BY
            posting_date
            ORDER BY
            posting_date DESC
    """,
    
    f"""
    SELECT
        SUM(dn.grand_total) AS grand_total
    FROM
        `tabDelivery Note` dn
    WHERE
        dn.docstatus = 1
        AND dn.is_return = 0
        AND name NOT IN (
            SELECT
                delivery_note
            FROM
                `tabSales Invoice Item`
            WHERE
                docstatus = 1
                AND delivery_note IS NOT NULL
                AND is_return = 0
            GROUP BY
                delivery_note
    )
    and customer=%s
    """,
    
    f"""
    
    SELECT
    posting_date,
    name AS sales_invoice,
    grand_total,
    outstanding_amount,
    DATEDIFF(CURDATE(), posting_date) AS age
FROM
    `tabSales Invoice`
WHERE
    docstatus = 1
    AND is_return = 0
    AND DATEDIFF(CURDATE(), posting_date) <= 720
    AND customer = %s
ORDER BY
    posting_date DESC,
    name DESC
    """
    ]
    def cus_data(queries,customer,ageing_df):
        result = [frappe.db.sql(q,customer, as_dict=True) for q in queries]
        ageing_df['Unbilled Notes']=result[2][0]['grand_total']
        ageing_df=ageing_df[['Outstanding','Unbilled Notes','>30','>45','>60','>90']]
        result[2]=ageing_df.to_dict(orient='records')
        return result


    result=cus_data(queries,customer,ageing_df)
    dic={
        'customer':result[0],
        'payment_entry':result[1],
        'ageing':result[2],
        'sales_invoice':result[3]
    }
    return dic
