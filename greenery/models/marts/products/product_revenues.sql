{{
    config(
        materialized='table'
    )
}}

WITH ProductRevenues AS (
    SELECT
        p.product_id,
        p.name AS ProductName,
        SUM(oi.quantity * p.price) AS Revenue
    FROM
        {{ source('postgres', 'stg_postgres__products') }} p
    JOIN
        {{ source('postgres', 'stg_postgres__order_items') }} oi ON p.product_id = oi.product_id
    GROUP BY
        p.product_id,
        p.name
)
SELECT
    ProductName,
    Revenue
FROM
    ProductRevenues
ORDER BY
    Revenue DESC