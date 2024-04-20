with num_page_views AS (
    SELECT session_id, COUNT(*) AS num_pages_visited,
    ARRAY_AGG(page_name) AS PAGES_VISITED,
    --FROM PROD.DEFAULT_BASE.BASE_PAGEVIEWS
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
               ROW_NUMBER() OVER (PARTITION BY SESSION_ID ORDER BY ORDER_AT_TS) AS order_row_number,
        FROM {{ref('BASE_ORDERS')}}
    ) AS subquery
    WHERE order_row_number = 1
),

num_orders AS (
    SELECT session_id, COUNT(*) AS num_orders_made
    FROM orders,
    --FROM PROD.DEFAULT_BASE.BASE_ORDERS
    GROUP BY session_id
),
session_info AS (
    SELECT  npv.session_id,
            num_pages_visited,
            pages_visited,
            num_item_visited,
            items_added_to_cart,
            SESSION_PRICE
    FROM num_page_views npv
    LEFT JOIN num_item_views niv ON npv.session_id = niv.session_id
),
session_info_with_orders AS (
    SELECT  si.session_id,
            num_pages_visited,
            pages_visited,
            num_item_visited,
            num_orders_made,
            items_added_to_cart,
            SESSION_PRICE
    FROM session_info si
    LEFT JOIN num_orders no ON si.session_id = no.session_id
)

SELECT bs.session_id,
    COALESCE(num_pages_visited, 0) as num_pages_visited,
    COALESCE(pages_visited,[]) as pages_visited,
    COALESCE(num_item_visited, 0) as num_item_visited,
    COALESCE(num_orders_made, 0) as num_orders_made,
    COALESCE(items_added_to_cart, 0) as items_added_to_cart,
    COALESCE(SESSION_PRICE,0) as SESSION_PRICE,
    IP,
    CLIENT_ID,
    SESSION_AT_TS,
    OS
FROM {{ref('BASE_SESSIONS')}} bs
INNER JOIN session_info_with_orders si ON si.session_id=bs.session_id

-- session price is regardless of whether order was made or not. We will only include the session_price 
-- in the expenses when an order was made

