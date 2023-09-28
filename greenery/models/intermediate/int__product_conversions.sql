{{
    config(
        materialized='table'
    )
}}

WITH valid_sessions AS (
  SELECT DISTINCT
    e.session_id,
    e.order_id
  FROM {{ ref('stg_postgres__events') }} e
  WHERE e.event_type IN ('add_to_cart', 'checkout')
    AND e.order_id IS NOT NULL
),
product_sessions AS (
  SELECT
    p.product_id,
    p.name AS product_name,
    e.session_id
  FROM {{ ref('stg_postgres__products') }} p
  JOIN {{ ref('stg_postgres__events') }} e ON p.product_id = e.product_id
  WHERE e.session_id IN (SELECT session_id FROM valid_sessions)
),
session_product_counts AS (
  SELECT
    ps.session_id,
    ps.product_id,
    COUNT(DISTINCT ps.product_id) AS product_count
  FROM product_sessions ps
  GROUP BY ps.session_id, ps.product_id
),
session_order_counts AS (
  SELECT
    vs.session_id,
    COUNT(DISTINCT vs.order_id) AS order_count
  FROM valid_sessions vs
  GROUP BY vs.session_id
)
SELECT
  ps.product_name,
  SUM(CASE WHEN spc.product_count > 0 THEN 1 ELSE 0 END) AS total_sessions_added_to_cart,
  COUNT(DISTINCT CASE WHEN soc.order_count > 0 THEN spc.session_id END) AS total_sessions_purchased,
  ROUND(
    CASE WHEN SUM(CASE WHEN spc.product_count > 0 THEN 1 ELSE 0 END) = 0 THEN 0.0
         ELSE COUNT(DISTINCT CASE WHEN soc.order_count > 0 THEN spc.session_id END)::FLOAT /
              SUM(CASE WHEN spc.product_count > 0 THEN 1 ELSE 0 END)
    END, 2
  ) AS conversion_rate
FROM product_sessions ps
JOIN session_product_counts spc ON ps.session_id = spc.session_id AND ps.product_id = spc.product_id
JOIN session_order_counts soc ON ps.session_id = soc.session_id
GROUP BY ps.product_name