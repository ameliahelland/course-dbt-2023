{{
    config(
        materialized='table'
    )
}}

SELECT
    u.user_id AS UserId,
    u.first_name AS FirstName,
    u.last_name AS LastName,
    CASE
        WHEN COUNT(o.order_id) >= 5 THEN 'High-Value'
        WHEN COUNT(o.order_id) >= 2 THEN 'Regular'
        ELSE 'Infrequent'
    END AS CustomerSegment,
    COUNT(o.order_id) as total_orders
FROM
    {{ ref('postgres.stg_postgres__users') }} AS u
LEFT JOIN
    {{ ref('postgres.stg_postgres__orders') }} AS o
ON
    u.user_id = o.user_id
GROUP BY
    u.user_id, u.first_name, u.last_name