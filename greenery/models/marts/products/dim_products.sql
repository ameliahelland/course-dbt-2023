{{
    config(
        materialized = 'table'
    )
}}

SELECT
    pios.ProductName,
    InventoryUsed,
    Inventory,
    AverageAmountPurchasedPerOrder,
    PageViews,
    Revenue
FROM
    {{ ref('int__product_inventory_and_order_statistics') }} AS pios
LEFT JOIN
    {{ ref('int__product_pageviews') }} AS pp
ON
    pios.ProductName = pp.ProductName
LEFT JOIN
    {{ ref('int__product_revenues') }} AS pr
ON
    pios.ProductName = pr.ProductName
