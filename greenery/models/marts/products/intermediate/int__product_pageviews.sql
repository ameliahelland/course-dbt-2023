{{
    config(
        materialized='table'
    )
}}

WITH ProductPageViews AS (
    SELECT
        p.product_id,
        p.name AS ProductName,
        COUNT(*) AS PageViews
    FROM
        {{ ref('stg_postgres__products') }} p
    JOIN
        {{ ref('stg_postgres__events') }} e ON p.product_id = e.product_id
    WHERE
        e.event_type = 'page_view'
    GROUP BY
        p.product_id,
        p.name
)
SELECT
    ProductName,
    PageViews
FROM
    ProductPageViews
ORDER BY
    PageViews DESC