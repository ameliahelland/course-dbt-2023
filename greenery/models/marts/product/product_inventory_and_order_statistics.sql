{{
    config(
        materialized='table'
    )
}}

WITH product_inventory AS (
    SELECT
        p.name AS product_name,
        SUM(oi.quantity) AS inventory_used,
        p.inventory,
        COUNT(DISTINCT oi.order_id) AS count_of_distinct_orders
    FROM {{ ref('postgres.stg_postgres__order_items') }} oi
    JOIN {{ ref('postgres.stg_postgres__products') }} p
    ON oi.product_id = p.product_id
    GROUP BY ALL
)
SELECT
    product_name,
    inventory_used,
    inventory,
    ROUND(inventory_used / count_of_distinct_orders, 2) AS average_amount_purchased_per_order
FROM product_inventory
ORDER BY inventory - inventory_used ASC