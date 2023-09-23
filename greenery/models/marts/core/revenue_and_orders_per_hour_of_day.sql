{{
    config(
        materialized='table'
    )
}}

SELECT
    TRUNC(TO_DATE(created_at), 'DAY') AS OrderDay,
    DATE_PART(HOUR, created_at) AS OrderHour,
    SUM(order_total) AS TotalRevenue,
    ROUND(AVG(order_total), 2) AS AverageOrderTotal,
    COUNT(order_id) AS OrdersPerHour
FROM
    {{ ref('postgres.stg_postgres__orders') }}
GROUP BY
    OrderDay, OrderHour
ORDER BY
    OrderDay, OrderHour
