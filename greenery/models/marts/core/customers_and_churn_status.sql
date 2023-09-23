{{
    config(
        materialized='table'
    )
}}

SELECT
    u.user_id AS UserId,
    u.first_name AS FirstName,
    u.last_name AS LastName,
    a.state,
    a.zipcode,
    CASE
        WHEN last_order_date IS NULL OR last_order_date < TO_DATE('2021-02-11') - INTERVAL '90 days' THEN 'Churned'
        ELSE 'Active'
    END AS ChurnStatus
FROM
    {{ source('postgres', 'users') }} AS u
LEFT JOIN (
    SELECT
        UserId,
        MAX(created_at) AS last_order_date
    FROM
        {{ source('postgres', 'orders') }}
    GROUP BY
        UserId
) AS last_order
ON
    u.user_id = last_order.UserId
LEFT JOIN 
    {{ source('postgres', 'addresses') }} a
ON
    a.address_id = u.address_id