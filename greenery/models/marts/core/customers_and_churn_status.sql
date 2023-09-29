{{
    config(
        materialized='table'
    )
}}

SELECT
    u.user_id,
    u.first_name,
    u.last_name,
    a.state,
    a.zipcode,
    CASE
        WHEN last_order_date IS NULL 
            OR last_order_date < ({{max_date('stg_postgres__orders', 'created_at')}}) - INTERVAL '90 days' 
        THEN 'Churned' -- TO_DATE('2021-02-11') - INTERVAL '90 days' THEN 'Churned'
        ELSE 'Active'
    END AS churn_status
FROM
    {{ ref('stg_postgres__users') }} AS u
LEFT JOIN (
    SELECT
        o.user_id,
        MAX(created_at) AS last_order_date
    FROM
        {{ ref('stg_postgres__orders') }} AS o
    GROUP BY
        o.user_id
) AS last_order
ON
    u.user_id = last_order.user_id
LEFT JOIN 
    {{ ref('stg_postgres__addresses') }} a
ON
    a.address_id = u.address_id