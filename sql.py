"""
Place for all the SQL needed by this. It was getting too messy in the main collections.py file.
"""
agency_name_sql = """select name 
from ns_import.rpt_ns_collection_agencies 
where id = %s"""

merchant_name_sql = """select name
from ns_import.rpt_merchants
where ns_customer_id = %s"""

agency1_sql = """	SELECT coll.ns_customer_id, coll.agency1_id, sum(pay.amount) as amount
FROM ns_import.rpt_collections coll
JOIN ns_import.rpt_funded_deals deals on coll.ns_customer_id = deals.merchant_id :: integer
JOIN ns_import.rpt_invoice_payments pay on deals.invoice_id = pay.invoice_id
WHERE coll.agency1_date is not null 
AND %s @> pay.payment_date
AND pay.payment_date >= coll.agency1_date
AND (coll.agency2_date is null OR pay.payment_date < coll.agency2_date)
GROUP BY coll.ns_customer_id
"""

overcollections1_sql = """SELECT sum(overcoll.credit_amount)
FROM ns_import.rpt_overcollections overcoll 
JOIN ns_import.rpt_collections coll ON overcoll.merchant::integer = coll.ns_customer_id
WHERE overcoll.merchant::integer = %s
AND overcoll.transaction_type='_deposit' 
AND coll.agency1_date is not null
AND overcoll.date >= coll.agency1_date
AND (coll.agency2_date is null OR overcoll.date <= coll.agency2_date)
AND %s @> overcoll.date
"""

agency2_sql = """SELECT coll.ns_customer_id, coll.agency2_id, sum(pay.amount)
FROM ns_import.rpt_collections coll
JOIN ns_import.rpt_funded_deals deals on coll.ns_customer_id = deals.merchant_id::integer
JOIN ns_import.rpt_invoice_payments pay on deals.invoice_id = pay.invoice_id
WHERE coll.agency2_date is not null 
AND %s @> pay.payment_date
AND pay.payment_date >= coll.agency2_date
AND (coll.agency3_date is null OR pay.payment_date < coll.agency3_date)
GROUP BY coll.ns_customer_id
"""

overcollections2_sql = """SELECT sum(overcoll.credit_amount)
FROM ns_import.rpt_overcollections overcoll 
JOIN ns_import.rpt_collections coll ON overcoll.merchant::integer = coll.ns_customer_id
WHERE overcoll.merchant::integer = %s
AND overcoll.transaction_type='_deposit' 
AND coll.agency2_date is not null
AND overcoll.date >= coll.agency2_date
AND (coll.agency3_date is null OR overcoll.date <= coll.agency3_date)
AND %s @> overcoll.date"""

agency3_sql = """SELECT coll.ns_customer_id, coll.agency3_id, sum(pay.amount)
FROM ns_import.rpt_collections coll
JOIN ns_import.rpt_funded_deals deals on coll.ns_customer_id = deals.merchant_id::integer
JOIN ns_import.rpt_invoice_payments pay on deals.invoice_id = pay.invoice_id
WHERE coll.agency3_date is not null 
AND %s @> pay.payment_date
AND pay.payment_date >= coll.agency3_date
AND (coll.agency4_date is null OR pay.payment_date < coll.agency4_date)
GROUP BY coll.ns_customer_id"""

overcollections3_sql = """SELECT sum(overcoll.credit_amount)
FROM ns_import.rpt_overcollections overcoll 
JOIN ns_import.rpt_collections coll ON overcoll.merchant::integer = coll.ns_customer_id
WHERE overcoll.merchant::integer = %s
AND overcoll.transaction_type='_deposit' 
AND coll.agency3_date is not null
AND overcoll.date >= coll.agency3_date
AND (coll.agency4_date is null OR overcoll.date <= coll.agency4_date)
AND %s @> overcoll.date
"""

agency4_sql = """SELECT coll.ns_customer_id, coll.agency4_id, sum(pay.amount)
FROM ns_import.rpt_collections coll
JOIN ns_import.rpt_funded_deals deals on coll.ns_customer_id = deals.merchant_id::integer
JOIN ns_import.rpt_invoice_payments pay on deals.invoice_id = pay.invoice_id
WHERE coll.agency4_date is not null 
AND %s @> pay.payment_date
AND pay.payment_date >= coll.agency4_date
GROUP BY coll.ns_customer_id"""

overcollections4_sql = """SELECT overcoll.merchant, sum(overcoll.credit_amount)
FROM ns_import.rpt_overcollections overcoll 
JOIN ns_import.rpt_collections coll ON overcoll.merchant::integer = coll.ns_customer_id
WHERE overcoll.merchant::integer = %s
AND overcoll.transaction_type='_deposit' 
AND coll.agency4_date is not null
AND overcoll.date >= coll.agency4_date
AND %s @> overcoll.date
"""

totalovercollection_sql = """select merchant_id::integer,collection_agency,0 as Net_collection,Overcollection_Deposits
 from
     (SELECT overcoll.merchant as merchant_id, SUM(overcoll.credit_amount) as Overcollection_Deposits
     FROM ns_import.rpt_overcollections overcoll  
          JOIN ns_import.rpt_collections coll ON overcoll.merchant::integer = coll.ns_customer_id
     WHERE overcoll.transaction_type='_deposit' 
     AND coll.agency1_date is not null 
     AND overcoll.date>=coll.agency1_date 
     AND  %s @> overcoll.date
     GROUP BY overcoll.merchant)overdeposit
     JOIN ns_import.rpt_collections coll ON overdeposit.merchant_id::integer = coll.ns_customer_id"""

cohort_sql = """SELECT merch.name as merchant_name, 
CASE WHEN merch.collections_date - max(payment_date) <= 14 
     THEN 'cohort1'
     ELSE 'cohort2' 
     END cohort
FROM ns_import.rpt_funded_deals fdeal
    JOIN ns_import.rpt_invoice_payments invo ON fdeal.invoice_id = invo.invoice_id
    JOIN ns_import.rpt_merchants merch ON fdeal.merchant_id::integer = merch.ns_customer_id
WHERE invo.payment_date < merch.collections_date 
    AND  merch.ns_customer_id = %s
    GROUP BY merch.ns_customer_id"""

merchant_collection_insert = """INSERT INTO ns_import.rpt_merchant_collections(merchant_id, merchant_name, cohort, agency_name, agency_type, 
net_receipts, overcollections, gross_receipts) 
VALUES (%s, %s, %s, %s, %s, 
        %s, %s, %s)"""