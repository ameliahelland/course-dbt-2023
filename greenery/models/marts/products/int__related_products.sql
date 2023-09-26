-- {{
--     config(
--         materialized='table'
--     )
-- }}

-- WITH ProductOrderCounts AS (
--     SELECT
--         p.name AS ProductName,
--         oi.product_id AS ProductID,
--         op.product_id AS RelatedProductID,
--         COUNT(*) AS Frequency
--     FROM
--         {{ ref('stg_postgres__order_items') }} oi
--     JOIN
--         {{ ref('stg_postgres__products') }} p ON oi.product_id = p.product_id
--     JOIN
--         {{ ref('stg_postgres__order_items') }} op ON oi.order_id = op.order_id
--     WHERE
--         oi.product_id <> op.product_id
--     GROUP BY
--         p.name, oi.product_id, op.product_id
-- ),
-- RankedProductPairs AS (
--     SELECT
--         *,
--         ROW_NUMBER() OVER (PARTITION BY ProductID ORDER BY Frequency DESC) AS RowNum
--     FROM
--         ProductOrderCounts
-- )
-- SELECT
--     r.ProductName AS BaseProduct,
--     p.name AS FrequentlyOrderedWithProduct,
--     r.Frequency
-- FROM
--     RankedProductPairs r
-- JOIN
--     {{ ref('stg_postgres__products') }} p ON r.RelatedProductID = p.product_id
-- WHERE
--     r.RowNum = 1
-- ORDER BY
--     r.Frequency DESC


WITH BaseProducts AS (
    SELECT
        DISTINCT ProductName
    FROM {{}}
),
ProductOrderCounts AS (
    SELECT
        tpn.ProductName AS BaseProduct,
        oi.ProductName AS FrequentlyOrderedWithProduct,
        COUNT(*) AS Frequency
    FROM
        {{ ref('int__related_products') }} rp
    JOIN
        BaseProducts bp ON rp.BaseProduct = bp.ProductName
    JOIN
        BaseProducts oi ON rp.FrequentlyOrderedWithProduct = oi.ProductName
    WHERE
        bp.ProductName <> oi.ProductName
    GROUP BY
        bp.ProductName, oi.ProductName
),
RankedProductPairs AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY BaseProduct ORDER BY Frequency DESC) AS RowNum
    FROM
        ProductOrderCounts
)
SELECT
    BaseProduct AS ProductName,
    MAX(CASE WHEN RowNum = 1 THEN FrequentlyOrderedWithProduct END) AS MostPopularItem1,
    MAX(CASE WHEN RowNum = 2 THEN FrequentlyOrderedWithProduct END) AS MostPopularItem2,
    MAX(CASE WHEN RowNum = 3 THEN FrequentlyOrderedWithProduct END) AS MostPopularItem3,
    MAX(CASE WHEN RowNum = 4 THEN FrequentlyOrderedWithProduct END) AS MostPopularItem4,
    MAX(CASE WHEN RowNum = 5 THEN FrequentlyOrderedWithProduct END) AS MostPopularItem5,
    MAX(CASE WHEN RowNum = 1 THEN Frequency END) AS FrequencyItem1,
    MAX(CASE WHEN RowNum = 2 THEN Frequency END) AS FrequencyItem2,
    MAX(CASE WHEN RowNum = 3 THEN Frequency END) AS FrequencyItem3,
    MAX(CASE WHEN RowNum = 4 THEN Frequency END) AS FrequencyItem4,
    MAX(CASE WHEN RowNum = 5 THEN Frequency END) AS FrequencyItem5
FROM
    RankedProductPairs
GROUP BY
    BaseProduct
ORDER BY
    BaseProduct