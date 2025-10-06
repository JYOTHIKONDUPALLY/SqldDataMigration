-- Top spending customers (top 10, 20 50 etc) All Customers
SELECT 
    customer_name,
    SUM(total_amount) AS total_spent
FROM 
    invoice_details
GROUP BY 
    customer_name
ORDER BY 
    total_spent DESC
LIMIT 50;

-- Top spending customers (top 10, 20 50 etc) Members 
SELECT 
    d.customer_name,
    SUM(d.total_amount) AS total_spent
FROM 
    invoice_details AS d
WHERE 
    d.is_member = 1
GROUP BY 
     d.customer_name
ORDER BY 
    total_spent DESC
LIMIT 10

-- Top spending customers (top 10, 20 50 etc)  Non-Members (Filter option)

SELECT 
    d.customer_name,
    SUM(d.total_amount) AS total_spent
FROM 
    invoice_details AS d
WHERE 
    d.is_member = 0
GROUP BY 
     d.customer_name
ORDER BY 
    total_spent DESC
LIMIT 10


-- What are the top 5 products (by spend) bought by their top spending customers.
WITH top_customers AS (
    SELECT customer_id, customer_name, SUM(total_amount) AS total_spent
    FROM invoice_details
    GROUP BY customer_id, customer_name
    ORDER BY total_spent DESC
    LIMIT 5
),
ranked_products AS (
    SELECT
        tc.customer_id,
        tc.customer_name,
        iid.item_name,
        SUM(iid.total_price) AS total_spent,
        ROW_NUMBER() OVER (
            PARTITION BY tc.customer_id
            ORDER BY SUM(iid.total_price) DESC
        ) AS rn
    FROM top_customers AS tc
    INNER JOIN invoice_details AS id
        ON tc.customer_id = id.customer_id
    INNER JOIN invoice_items_detail AS iid
        ON iid.invoice_id = id.id
    WHERE iid.item_type = 'product'
    GROUP BY tc.customer_id, tc.customer_name, iid.item_name
) select * from ranked_products rp where rn<=5 ORDER BY  rn ASC;

-- What are the top 5 items/things that they top spending customers buy? (it could be a range service, product or class) – grouped by item name

WITH top_customers AS (
    SELECT customer_id, customer_name, SUM(total_amount) AS total_spent
    FROM invoice_details
    GROUP BY customer_id, customer_name
    ORDER BY total_spent DESC
    LIMIT 5
),
ranked_Items AS (
    SELECT
        tc.customer_id,
        tc.customer_name,
        iid.item_name,
        SUM(iid.total_price) AS total_spent,
        ROW_NUMBER() OVER (
            PARTITION BY tc.customer_id
            ORDER BY SUM(iid.total_price) DESC
        ) AS rn
    FROM top_customers AS tc
    INNER JOIN invoice_details AS id
        ON tc.customer_id = id.customer_id
    INNER JOIN invoice_items_detail AS iid
        ON iid.invoice_id = id.id
    GROUP BY tc.customer_id, tc.customer_name, iid.item_name
) select * from ranked_Items rp where rn<=5 ORDER BY  rn ASC;


-- For Top Non-Members (by Spend) How much they would have saved if they had basic (individual membership) – is this possible to assess? So that they can use this for marketing

SELECT 
    id.customer_id,
    id.customer_name,
    COUNT(DISTINCT id.id) AS total_invoices,
    SUM(id.total_amount) AS total_spent,
    SUM(iid.total_price * iid.membership_discount / 100) AS potential_savings,
    ROUND((SUM(iid.total_price * iid.membership_discount / 100) / SUM(id.total_amount)) * 100, 2) AS savings_percentage
FROM 
    invoice_details id
INNER JOIN 
    invoice_items_detail iid ON id.id = iid.invoice_id
WHERE 
    id.is_member = 0
GROUP BY 
    id.customer_id, id.customer_name
ORDER BY 
    total_spent DESC
LIMIT 10;


--What service (in order – top 5) gets me the most customers (new customers)
SELECT 
    iid.item_name AS service_name,
    COUNT(DISTINCT id.customer_id) AS new_customers
FROM 
    invoice_details id
JOIN 
    invoice_items_detail iid ON id.id = iid.invoice_id
WHERE 
    id.invoice_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR) -- Considering new customers from the 3 last year
    AND iid.item_type IN ('service', 'appointment','advancebookingfee') AND
id.customer_id NOT IN (SELECT DISTINCT customer_id FROM invoice_details WHERE invoice_date < DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
GROUP BY 
    iid.item_name
ORDER BY 
    new_customers DESC
LIMIT 5;

--What customers were added during a time period and what was their first service/appointment purchased  Give me all customers who bought/used a particular service first?

SELECT id AS customer_id, customer_name, customer_email, 
MIN(invoice_date) AS first_purchase_date, ii.item_name AS first_service 
FROM invoice_details AS id INNER JOIN invoice_items_detail AS ii ON id.id = ii.invoice_id
WHERE ((invoice_date >= '2023-01-01') AND (invoice_date <= '2023-12-31'))
AND (ii.item_type = 'service') 
GROUP BY customer_id, customer_name, customer_email , ii.item_name
ORDER BY first_purchase_date ASC



--   What month is the busiest (based on filter above – all customers, members, non-members)
SELECT
    toStartOfMonth(invoice_date) AS month_start_date,
     toMonth(invoice_date) AS month_number,
CASE toMonth(invoice_date)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_name,
    COUNTDistinct(customer_id) AS customer_count 
FROM
    invoice_details 
WHERE
    toStartOfYear(invoice_date) = toDate('2023-01-01')
GROUP BY
    month_start_date, month_number
ORDER BY
    customer_count DESC
LIMIT 5;


--members
SELECT
    toStartOfMonth(invoice_date) AS month_start_date,
     toMonth(invoice_date) AS month_number,
CASE toMonth(invoice_date)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_name,
    COUNTDistinct(customer_id) AS customer_count 
FROM
    invoice_details 
WHERE
    toStartOfYear(invoice_date) = toDate('2023-01-01')
    and is_member = 1
GROUP BY
    month_start_date, month_number
ORDER BY
    customer_count DESC
LIMIT 5;

--non-members
SELECT
    toStartOfMonth(invoice_date) AS month_start_date,
     toMonth(invoice_date) AS month_number,
CASE toMonth(invoice_date)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_name,
    COUNTDistinct(customer_id) AS customer_count 
FROM
    invoice_details 
WHERE
    toStartOfYear(invoice_date) = toDate('2023-01-01')
    and is_member = 0
GROUP BY
    month_start_date, month_number
ORDER BY
    customer_count DESC
LIMIT 5;


--  What Day of the week (on average – last 12 months) is the busiest – most customers
SELECT 
    toDayOfWeek(invoice_date) AS day_of_week,
    CASE toDayOfWeek(invoice_date)
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_of_week_name,
    COUNT(DISTINCT customer_id) AS customer_count
FROM 
    invoice_details 
WHERE 
    invoice_date >= today() - interval 12 month
GROUP BY 
    day_of_week
ORDER BY 
    customer_count DESC
LIMIT 5;


--What service (if more than one) is
WITH top_days AS (
    -- 1. Find the top 5 days by distinct customer count (Aggregation Step)
    SELECT
        toDayOfWeek(invoice_date) AS day_of_week_num,
        COUNT(DISTINCT customer_id) AS customer_count
    FROM 
        invoice_details
    WHERE 
        invoice_date >= today() - INTERVAL 12 MONTH
    GROUP BY 
        day_of_week_num
    ORDER BY
        customer_count DESC
    LIMIT 1
)
-- 2. Join the top days back to the original data to get item details
SELECT 
    td.customer_count,
    CASE td.day_of_week_num
        WHEN 1 THEN 'Monday'
        when 2 Then 'Tuesday'
        when 3 then 'wednessday'
        when 4 then 'thursday'
        when 5 then 'friday'
        when 6 then 'saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_of_week,
    id.invoice_number,
    iid.item_type, 
    iid.item_name,
    id.invoice_date
FROM 
    invoice_details id
INNER JOIN 
    invoice_items_detail AS iid ON id.id = iid.invoice_id
INNER JOIN 
    top_days AS td ON toDayOfWeek(id.invoice_date) = td.day_of_week_num
    where id.invoice_date >= today() - INTERVAL 12 MONTH
ORDER BY 
    td.customer_count DESC, id.invoice_date DESC
LIMIT 10 -- Limit the final output as this can be huge


--  What are the most popular classes (sold most or most in demand)
SELECT 
    ii.item_name,
    SUM(ii.quantity) AS total_quantity
FROM 
    invoice_items_detail ii
JOIN 
    invoice_details id ON ii.invoice_id = id.id
WHERE 
    id.status = '1'
    and ii.item_type ='class'
GROUP BY 
    ii.item_name
ORDER BY 
    total_quantity DESC
LIMIT 10;


--What classes (top 5) gives me the most NEW customers (first time customers and they came in for classes without buying a service or product before)
SELECT 
    ii.category AS class_category, 
    COUNT(DISTINCT i.customer_id) AS new_customers_count
FROM 
    invoice_details i
JOIN 
    invoice_items_detail ii ON i.id = ii.invoice_id
WHERE 
    ii.item_type = 'class'
    AND i.id NOT IN (
        SELECT invoice_id 
        FROM invoice_items_detail 
        WHERE item_type != 'class'
    )
GROUP BY 
    ii.category
ORDER BY 
    new_customers_count DESC
LIMIT 5;


--   What customers spend most on classes (have taken multiple classes) – in order (top 100 customers)
SELECT 
    id AS customer_id,
    customer_name,
    SUM(total_amount) AS total_spent,
    COUNT(DISTINCT invoice_id) AS classes_taken
FROM 
    invoice_details
JOIN 
    invoice_items_detail ON invoice_details.id = invoice_items_detail.invoice_id
WHERE 
    item_type = 'class'
GROUP BY 
    customer_id, customer_name
HAVING 
    -- classes_taken > 1
ORDER BY 
    total_spent DESC
LIMIT 100;


--What customers were added during a time period and what was their first product purchased (and it was very first item – no other services, classes or membership or any other item was purchased by them before the product purchase)

SELECT 
    id AS customer_id,
    customer_name,
    customer_email,
    MIN(invoice_date) AS first_purchase_date,
    item_name AS first_product_name
FROM 
    invoice_details
JOIN 
    invoice_items_detail ON invoice_details.id = invoice_items_detail.invoice_id
WHERE 
    invoice_details.invoice_date BETWEEN '2023-01-01' AND '2023-12-31' 
    AND item_type = 'product'
GROUP BY 
    id, customer_name, customer_email, item_name
HAVING 
    COUNT(CASE WHEN item_type != 'product' THEN 1 END) = 0
ORDER BY 
    first_purchase_date;



    --    What product (in order – opt 5 or 10) gives me the most new customers

    SELECT 
    i.item_name,
    COUNT(DISTINCT d.customer_id) AS new_customers_count
FROM 
    invoice_items_detail i
JOIN 
    invoice_details d ON i.invoice_id = d.id
WHERE 
    d.is_member = 'no'
GROUP BY 
    i.item_name
ORDER BY 
    new_customers_count DESC
LIMIT 10;


--Top 10 products that is sold the most (to all, to members, only to non-members) – exclusions needed as filters (you may want to exclude certain products like targets which gets sold always in large numbers but not a significant source of revenue because targets are inexpensive)

WITH product_sales AS (
    SELECT 
        iid.item_name,
        SUM(iid.quantity) AS total_quantity,
        SUM(iid.total_price) AS total_revenue,
        ed.is_member
    FROM 
        invoice_items_detail iid
    JOIN 
        invoice_details ed ON iid.invoice_id = ed.id
    WHERE 
        iid.item_name NOT LIKE '%target%'
    GROUP BY 
        iid.item_name, ed.is_member
)

SELECT
    item_name,
    total_quantity,
    total_revenue,
    is_member
FROM 
    product_sales
ORDER BY 
    total_quantity DESC
LIMIT 10;


--  (top 10) Products what is ordered the most (high turnover)

SELECT 
    iid.item_name, 
    SUM(iid.quantity) AS total_quantity_ordered
FROM 
    invoice_items_detail iid
JOIN 
    invoice_details id ON iid.invoice_id = id.id
WHERE 
    id.status = '1'
GROUP BY 
    iid.item_name
ORDER BY 
    total_quantity_ordered DESC
LIMIT 10


--  What product is purchased the least? (top 10 or 20 or 50)
SELECT item_name, SUM(quantity) AS total_quantity
FROM invoice_items_detail
GROUP BY item_name
ORDER BY total_quantity ASC
LIMIT 10;



-- What product stays on the shelf the longest (top 10 or 20 or 50)
SELECT 
    iid.id AS item_id, 
    iid.item_name, 
    SUM(iid.quantity) AS total_quantity, 
    MAX(id.invoice_date) AS last_invoice_date, 
    COUNT(*) AS total_sales, 
    dateDiff('day', MAX(id.invoice_date), now()) AS days_on_shelf 
FROM 
    invoice_items_detail iid
INNER JOIN 
    invoice_details id ON iid.invoice_id = id.id 
WHERE 
    id.status = '1' -- Assuming '1' means a valid/completed invoice
GROUP BY 
    iid.id, iid.item_name 
ORDER BY 
    days_on_shelf DESC 
LIMIT 50;