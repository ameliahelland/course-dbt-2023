{{
    config(
        materialized='table'
    )
}}

SELECT
    u.user_id,
    u.first_name,
    u.last_name,
    CASE
        WHEN COUNT(o.order_id) >= 5 THEN 'High-Value'
        WHEN COUNT(o.order_id) >= 2 THEN 'Regular'
        ELSE 'Infrequent'
    END AS customer_segment,
    COUNT(o.order_id) as total_orders
FROM
    {{ ref('stg_postgres__users') }} AS u
LEFT JOIN
    {{ ref('stg_postgres__orders') }} AS o
ON
    u.user_id = o.user_id
GROUP BY
    u.user_id, u.first_name, u.last_name