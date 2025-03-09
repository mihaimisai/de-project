SELECT 
    fso.sales_record_id,
    fso.sales_order_id,
    dc.counterparty_legal_name,  
    dcu.currency_name,
    dcu.currency_code,
    fso.unit_price,
    fso.units_sold
FROM fact_sales_order AS fso
LEFT JOIN dim_currency AS dcu ON
fso.currency_id = dcu.currency_id
LEFT JOIN dim_counterparty AS dc ON
fso.counterparty_id = dc.counterparty_id
ORDER BY 
dc.counterparty_legal_name;





SELECT 
    dc.counterparty_legal_name,  
    dcu.currency_name,
    fso.unit_price,
    SUM(fso.units_sold) AS total_units_sold,
    SUM(fso.units_sold*fso.unit_price) AS total_return
FROM fact_sales_order AS fso
LEFT JOIN dim_currency AS dcu ON
fso.currency_id = dcu.currency_id
LEFT JOIN dim_counterparty AS dc ON
fso.counterparty_id = dc.counterparty_id
GROUP BY 
    dc.counterparty_legal_name,
    fso.unit_price,
    dcu.currency_name;


SELECT column_name, data_type
FROM information_schema.columns 
WHERE table_name = 'fact_sales_order';




SELECT 
    dc.counterparty_legal_name,   
    SUM(fso.units_sold*fso.unit_price) AS total_return
FROM fact_sales_order AS fso
LEFT JOIN dim_staff AS ds ON
fso.sales_staff_id = ds.staff_id
LEFT JOIN dim_counterparty AS dc ON
fso.counterparty_id = dc.counterparty_id
GROUP BY 
    dc.counterparty_legal_name
ORDER BY
    total_return DESC;


-- query for paied orders currency and payment date
SELECT
    p.payment_id,
    cp.counterparty_legal_name,
    p.paid,
    c.currency_code,
    p.payment_date,
    so.units_sold,
    so.unit_price,
    so.units_sold*so.unit_price AS total_return
FROM payment AS p
LEFT JOIN currency AS c ON
p.currency_id = c.currency_id
LEFT JOIN transaction AS t ON
p.transaction_id = t.transaction_id
INNER JOIN sales_order AS so ON  
t.sales_order_id = so.sales_order_id
INNER JOIN counterparty AS cp ON
so.counterparty_id = cp.counterparty_id
WHERE p.paid = True  
ORDER BY p.payment_date DESC;


-- query for paied orders currency and payment date
SELECT
    p.payment_id,
    so.sales_order_id,
    p.paid,
    c.currency_code,
    so.agreed_payment_date,
    so.units_sold,
    so.unit_price,
    so.units_sold*so.unit_price AS total_return
FROM payment AS p
LEFT JOIN currency AS c ON
p.currency_id = c.currency_id
LEFT JOIN transaction AS t ON
p.transaction_id = t.transaction_id
INNER JOIN sales_order AS so ON  
t.sales_order_id = so.sales_order_id

WHERE p.paid = True  
ORDER BY p.payment_date DESC;