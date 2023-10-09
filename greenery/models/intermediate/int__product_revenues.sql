{{
    config(
        materialized='table'
    )
}}

WITH ProductRevenues AS (
    SELECT
        p.product_id,
        p.name AS product_name,
        SUM(oi.quantity * p.price) AS revenue
    FROM
        {{ ref('stg_postgres__products') }} p
    LEFT JOIN
        {{ ref('stg_postgres__order_items') }} oi ON p.product_id = oi.product_id
    GROUP BY
        p.product_id,
        p.name
)
SELECT
    product_name,
    revenue
FROM
    ProductRevenues
ORDER BY
    revenue DESC