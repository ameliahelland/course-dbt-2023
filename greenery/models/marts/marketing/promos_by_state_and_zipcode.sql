{{
    config(
        materialized='table'
    )
}}

WITH PromoCounts AS (
    SELECT
        a.state,
        a.zipcode,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN o.promo_id IS NOT NULL THEN o.order_id END) AS promo_orders
    FROM
        {{ ref('stg_postgres__orders') }} AS o
    JOIN
        {{ ref('stg_postgres__addresses') }} AS a
    ON
        o.address_id = a.address_id
    GROUP BY
        a.state,
        a.zipcode
)
SELECT
    pc.state,
    pc.zipcode,
    pc.total_orders AS TotalOrders,
    pc.promo_orders AS PromoOrders,
    CASE
        WHEN pc.total_orders > 0 THEN Round((pc.promo_orders::FLOAT / pc.total_orders) * 100, 2)
        ELSE 0
    END AS promo_application_rate
FROM
    PromoCounts AS pc
ORDER BY
    promo_application_rate DESC