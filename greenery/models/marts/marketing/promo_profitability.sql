{{
    config(
        materialized='table'
    )
}}

SELECT
    p.promo_id,
    p.discount,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(order_total) AS total_revenue,
    (SUM(order_total) - (COUNT(DISTINCT o.order_id) * p.discount)) AS CampaignProfit
FROM
    {{ ref('stg_postgres__promos') }} AS p
LEFT JOIN
    {{ ref('stg_postgres__orders') }} AS o
ON
    p.promo_id = o.promo_id
GROUP BY
    p.promo_id, p.discount