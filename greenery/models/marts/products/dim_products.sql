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
    Revenue,
    MostPopularItem1,
    FrequencyItem1,
    MostPopularItem2,
    FrequencyItem2,
    MostPopularItem3,
    FrequencyItem3,
    MostPopularItem4,
    FrequencyItem4,
    MostPopularItem5,
    FrequencyItem5

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
LEFT JOIN
    {{ ref('int__related_products') }} AS rp
ON
    pios.ProductName = rp.ProductName
