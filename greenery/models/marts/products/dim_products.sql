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
    {{ ref('int__product_inventory_and_order_statistics') }} pios
LEFT JOIN
    {{ ref('int__product_pageviews') }} pp
ON
    pios.ProductName = pp.ProductName
LEFT JOIN
    {{ ref('int__product_revenues') }} pr
ON
    pios.ProductName = pr.ProductName
