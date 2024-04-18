SELECT item_name, 
       COUNT(*) AS item_count, -- Adding a count of each item_name
       ARRAY_AGG(price_per_unit) AS price_array
FROM {{ref('BASE_ITEM_VIEWS')}}
GROUP BY item_name
