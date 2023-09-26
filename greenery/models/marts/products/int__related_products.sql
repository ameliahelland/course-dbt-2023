{{
    config(
        materialized='table'
    )
}}

WITH ProductOrderSummary AS (
    SELECT
        p.Name AS BaseProduct,
        p2.Name AS OrderedWithProduct,
        COUNT(*) AS Frequency,
        ROW_NUMBER() OVER (PARTITION BY p.Name ORDER BY COUNT(*) DESC) AS RowNum
    FROM
        {{ ref('stg_postgres__order_items') }} oi
    JOIN
        {{ ref('stg_postgres__products') }} p ON oi.product_id = p.product_id
    JOIN
        {{ ref('stg_postgres__products') }} p2 ON oi.product_id = p2.product_id
    WHERE
        p.Name <> p2.Name
    GROUP BY
        p.Name, p2.Name
)
SELECT
    BaseProduct,
    MAX(CASE WHEN RowNum = 1 THEN OrderedWithProduct END) AS TopProduct1,
    MAX(CASE WHEN RowNum = 1 THEN Frequency END) AS Frequency1,
    MAX(CASE WHEN RowNum = 2 THEN OrderedWithProduct END) AS TopProduct2,
    MAX(CASE WHEN RowNum = 2 THEN Frequency END) AS Frequency2,
    MAX(CASE WHEN RowNum = 3 THEN OrderedWithProduct END) AS TopProduct3,
    MAX(CASE WHEN RowNum = 3 THEN Frequency END) AS Frequency3
FROM
    ProductOrderSummary
WHERE
    RowNum <= 3
GROUP BY
    BaseProduct
ORDER BY
    BaseProduct