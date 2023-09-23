{{
    config(
        materialized='table'
    )
}}

WITH product_inventory AS (
    SELECT
        p.name AS ProductName,
        SUM(oi.quantity) AS InventoryUsed,
        p.Inventory,
        COUNT(DISTINCT oi.order_id) AS CountOfDistinctOrders
    FROM {{ source('postgres', 'order_items') }} oi
    JOIN {{ source('postgres', 'products') }} p
    ON oi.product_id = p.product_id
    GROUP BY ALL
)
SELECT
    ProductName,
    InventoryUsed,
    Inventory,
    ROUND(InventoryUsed / CountOfDistinctOrders, 2) AS AverageAmountPurchasedPerOrder
FROM product_inventory
ORDER BY inventory - InventoryUsed ASC