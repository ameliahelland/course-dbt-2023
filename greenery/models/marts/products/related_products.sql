{{
    config(
        materialized='table'
    )
}}

WITH ProductOrderCounts AS (
    SELECT
        p.name AS ProductName,
        oi.product_id AS ProductID,
        op.product_id AS RelatedProductID,
        COUNT(*) AS Frequency
    FROM
        {{ source('postgres', 'order_items') }} oi
    JOIN
        {{ source('postgres', 'products') }} p ON oi.product_id = p.product_id
    JOIN
        {{ source('postgres', 'order_items') }} op ON oi.order_id = op.order_id
    WHERE
        oi.product_id <> op.product_id
    GROUP BY
        p.name, oi.product_id, op.product_id
),
RankedProductPairs AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY ProductID ORDER BY Frequency DESC) AS RowNum
    FROM
        ProductOrderCounts
)
SELECT
    r.ProductName AS BaseProduct,
    p.name AS FrequentlyOrderedWithProduct,
    r.Frequency
FROM
    RankedProductPairs r
JOIN
    {{ source('postgres', 'products') }} p ON r.RelatedProductID = p.product_id
WHERE
    r.RowNum = 1
ORDER BY
    r.Frequency DESC