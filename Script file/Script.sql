-- What are the top 5 products (by spend) bought by their top spending customers.
SELECT 
    iid.item_name, 
    SUM(iid.total_price) AS total_spent,
    id.customer_name,
    id.customer_id
FROM 
    invoice_details AS id
JOIN 
    invoice_items_detail AS iid ON iid.invoice_id = id.id
JOIN 
    (SELECT 
         customer_id, 
         SUM(total_amount) AS total_spent_customer
     FROM 
         invoice_details
     GROUP BY 
         customer_id
     ORDER BY 
         total_spent_customer DESC
     LIMIT 5) AS top_customers ON id.customer_id = top_customers.customer_id
GROUP BY 
    iid.item_name, id.customer_name, id.customer_id
ORDER BY 
    total_spent DESC
LIMIT 5;

-- What are the top 5 items/things that they top spending customers buy? (it could be a range service, product or class) – grouped by item name
SELECT 
    iid.item_name, 
    SUM(iid.total_price) AS total_spent
FROM 
    invoice_items_detail AS iid
JOIN 
    invoice_details AS id ON iid.invoice_id = id.id
WHERE 
    id.total_amount IN (
        SELECT 
            total_amount 
        FROM 
            invoice_details 
        ORDER BY 
            total_amount DESC 
        LIMIT 5
    )
GROUP BY 
    iid.item_name
ORDER BY 
    total_spent DESC
LIMIT 5;


-- For Top Non-Members (by Spend) How much they would have saved if they had basic (individual membership) – is this possible to assess? So that they can use this for marketing
SELECT 
    id AS invoice_id,
    customer_name,
    SUM(total_amount) AS total_spent,
    SUM(CASE 
            WHEN is_member = 0 THEN (total_amount * membership_discount / 100)
            ELSE 0 
        END) AS potential_savings
FROM 
    invoice_details
JOIN 
    invoice_items_detail ON invoice_details.id = invoice_items_detail.invoice_id
WHERE 
    is_member = 0
GROUP BY 
    id, customer_name
ORDER BY 
    total_spent DESC
LIMIT 10