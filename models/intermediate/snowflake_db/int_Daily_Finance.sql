with temp_orders AS
(
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY SESSION_ID ORDER BY ORDER_AT_TS) AS session_row_number,
        FROM {{ref('BASE_ORDERS')}}
    ) AS subquery
    WHERE session_row_number = 1
),

num_page_views AS (
    SELECT session_id, COUNT(*) AS num_pages_visited
    FROM {{ref('BASE_PAGEVIEWS')}}
    GROUP BY session_id
),
num_item_views AS (
    SELECT session_id, 
        COUNT(*) AS num_item_visited,
        SUM(ADD_TO_CART_QUANTITY) AS items_added_to_cart,
        SUM((ADD_TO_CART_QUANTITY-REMOVE_FROM_CART_QUANTITY)*PRICE_PER_UNIT) AS SESSION_PRICE
    FROM {{ref('BASE_ITEM_VIEWS')}}
    GROUP BY session_id
),
orders AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY ORDER_ID ORDER BY ORDER_AT_TS) AS order_row_number,
        FROM temp_orders
    ) AS subquery
    WHERE order_row_number = 1
),

num_orders AS (
    SELECT session_id, COUNT(*) AS num_orders_made,
    FROM orders,
    --FROM {{ref('BASE_ORDERS')}}
    GROUP BY session_id
),
session_info AS (
    SELECT  npv.session_id,
            num_pages_visited,
            num_item_visited,
            items_added_to_cart,
            SESSION_PRICE
    FROM num_page_views npv
    LEFT JOIN num_item_views niv ON npv.session_id = niv.session_id
),
session_info_with_orders AS (
    SELECT  si.session_id,
            num_pages_visited,
            num_item_visited,
            num_orders_made,
            items_added_to_cart,
            SESSION_PRICE
    FROM session_info si
    LEFT JOIN num_orders no ON si.session_id = no.session_id
),

int_sessions AS (
    SELECT bs.session_id,
    COALESCE(num_pages_visited, 0) as num_pages_visited,
    COALESCE(num_item_visited, 0) as num_item_visited,
    COALESCE(num_orders_made, 0) as num_orders_made,
    COALESCE(items_added_to_cart, 0) as items_added_to_cart,
    COALESCE(SESSION_PRICE,0) as SESSION_PRICE,
    IP,
    CLIENT_ID,
    SESSION_AT_TS,
    OS
FROM {{ref('BASE_SESSIONS')}} bs
LEFT JOIN session_info_with_orders si ON si.session_id=bs.session_id
),

int_orders as (
    SELECT orders.CLIENT_NAME,
    orders.ORDER_ID,
    orders.SESSION_ID,
    orders.ORDER_AT_TS,
    --SUBSTR(SHIPPING_COST_USD,4) AS SHIPPING_COST,
    orders.SHIPPING_COST_USD,
    orders.TAX_RATE,
    SESSION_PRICE,
FROM orders
--LEFT JOIN FIVETRAN_DATABASE.SNOWFLAKE_DB_WEB_SCHEMA.ITEM_VIEWS
LEFT JOIN int_sessions
--ON FIVETRAN_DATABASE.SNOWFLAKE_DB_WEB_SCHEMA.ORDERS.SESSION_ID = FIVETRAN_DATABASE.SNOWFLAKE_DB_WEB_SCHEMA.ITEM_VIEWS.SESSION_ID
ON int_sessions.SESSION_ID = orders.SESSION_ID
),
daily_expenses AS (SELECT "DATE" as expense_date,
    SUM(EXPENSE_AMOUNT) AS DAILY_EXPENSE,
FROM {{ref('BASE_EXPENSES')}}
GROUP BY expense_date),
unique_returns AS (
    WITH RankedOrders AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY ORDER_ID ORDER BY RETURNED_AT) AS rn
        FROM {{ref('BASE_RETURNS')}}
        WHERE is_refunded = 'yes'
    )
    SELECT ORDER_ID, RETURNED_AT
    FROM RankedOrders
    WHERE rn = 1
),
refund_orders AS (
SELECT int_orders.ORDER_ID,
    int_orders.CLIENT_NAME,
    int_orders.SESSION_ID,
    int_orders.ORDER_AT_TS,
    int_orders.SHIPPING_COST_USD,
    int_orders.TAX_RATE,
    SESSION_PRICE,
    returned_at,
    
from int_orders
LEFT JOIN unique_returns br
ON br.ORDER_ID=int_orders.ORDER_ID
),

refund_daily_expenses AS(
    SELECT RETURNED_AT,
    SUM(SESSION_PRICE*(1+TAX_RATE)+SHIPPING_COST_USD) AS TOTAL_PRICE,
    COUNT(*) AS NUM_ORDERS_REFUNDED
FROM refund_orders
WHERE RETURNED_AT IS NOT NULL
GROUP BY RETURNED_AT
),
daily_revenue AS(
SELECT
    DATE(ORDER_AT_TS) AS order_date,
    SUM(SESSION_PRICE*(1+TAX_RATE) + SHIPPING_COST_USD) AS daily_revenue
FROM int_orders
GROUP BY order_date
),
expense_and_revenue AS (
    SELECT * 
    FROM daily_revenue dr
    FULL JOIN daily_expenses de
    ON dr.order_date= de.expense_date
)

SELECT 
    ORDER_DATE AS "DATE",
    DAILY_REVENUE,
    DAILY_EXPENSE,
    COALESCE(TOTAL_PRICE, 0) AS DAILY_REFUND,
    DAILY_REVENUE - DAILY_EXPENSE - DAILY_REFUND AS DAILY_PROFIT,
    COALESCE(NUM_ORDERS_REFUNDED, 0) AS NUM_ORDERS_REFUNDED
FROM expense_and_revenue ear
FULL JOIN refund_daily_expenses rde
ON ear.ORDER_DATE = rde.RETURNED_AT