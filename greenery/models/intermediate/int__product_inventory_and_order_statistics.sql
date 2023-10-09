{{
    config(
        materialized='table'
    )
}}

WITH product_inventory AS (
    SELECT
        p.name AS product_name,
        p.product_id,
        p.inventory,
        SUM(oi.quantity) AS inventory_used,
        COUNT(DISTINCT oi.order_id) AS total_orders,
        SUM(oi.quantity * p.price) AS revenue
    FROM {{ ref('stg_postgres__order_items') }} oi
    LEFT JOIN {{ ref('stg_postgres__products') }} p
    ON oi.product_id = p.product_id
    GROUP BY ALL
)
SELECT
    product_name,
    product_id,
    inventory_used,
    inventory AS inventory_remaining,
    revenue,
    ROUND(inventory_used / total_orders, 2) AS average_amount_purchased_per_order
FROM product_inventory
ORDER BY inventory_remaining - inventory_used ASC