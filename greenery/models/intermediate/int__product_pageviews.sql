{{
    config(
        materialized='table'
    )
}}

WITH ProductPageViews AS (
    SELECT
        p.product_id,
        p.name AS product_name,
        COUNT(*) AS Pageviews
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
    product_name,
    PageViews
FROM
    ProductPageViews
ORDER BY
    PageViews DESC