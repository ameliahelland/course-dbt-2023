{{
    config(
        materialized='table'
    )
}}

SELECT
    TRUNC(TO_DATE(created_at), 'DAY') AS order_day,
    DATE_PART(HOUR, created_at) AS order_hour,
    SUM(order_total) AS total_revenue,
    ROUND(AVG(order_total), 2) AS average_order_total,
    COUNT(order_id) AS orders_per_hour
FROM
    {{ ref('stg_postgres__orders') }}
GROUP BY
    order_day, order_hour
ORDER BY
    order_day, order_hour
