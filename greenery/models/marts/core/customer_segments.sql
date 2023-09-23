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
    COUNT(o.order_id) as TotalOrders
FROM
    {{ source('postgres', 'users') }} AS u
LEFT JOIN
    {{ source('postgres', 'orders') }} AS o
ON
    u.user_id = o.user_id
GROUP BY
    u.user_id, u.first_name, u.last_name