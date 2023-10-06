{{
    config(
        materialized='table'
    )
}}

WITH product_inventory AS (
    SELECT
        p.name AS product_name,
        p.product_id,
        SUM(oi.quantity) AS inventory_used,
        p.inventory,
        COUNT(DISTINCT oi.order_id) AS total_orders
    FROM {{ ref('stg_postgres__order_items') }} oi
    JOIN {{ ref('stg_postgres__products') }} p
    ON oi.product_id = p.product_id
    GROUP BY ALL
)
SELECT
    product_name,
    product_id,
    inventory_used,
    inventory,
    ROUND(inventory_used / total_orders, 2) AS average_amount_purchased_per_order
FROM product_inventory
ORDER BY inventory - inventory_used ASC