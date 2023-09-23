{{
    config(
        materialized='table'
    )
}}

SELECT
    p.promo_id as PromoId,
    p.discount,
    COUNT(DISTINCT o.order_id) AS TotalOrders,
    SUM(order_total) AS TotalRevenue,
    (SUM(order_total) - (COUNT(DISTINCT o.order_id) * p.discount)) AS CampaignProfit
FROM
    {{ source('postgres', 'promos') }} AS p
LEFT JOIN
    {{ source('postgres', 'orders') }} AS o
ON
    p.promo_id = o.promo_id
GROUP BY
    p.promo_id, p.discount