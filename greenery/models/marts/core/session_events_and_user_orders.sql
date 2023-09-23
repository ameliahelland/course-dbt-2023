{{
    config(
        materialized='table'
    )
}}

SELECT
    e.session_id,
    e.page_url,
    e.event_type,
    e.created_at AS EventTimestamp,
    e.user_id,
    u.first_name AS UserFirstName,
    u.last_name AS UserLastName,
    u.email AS UserEmail,
    e.order_id,
    o.order_cost,
    o.shipping_cost,
    o.order_total,
    o.tracking_id,
    o.shipping_service,
    o.estimated_delivery_at,
    o.delivered_at,
    p.name AS ProductName
FROM
    {{ ref('postgres.stg_postgres__events') }} e
LEFT JOIN
    {{ ref('postgres.stg_postgres__users') }} u ON e.user_id = u.user_id
LEFT JOIN
    {{ ref('postgres.stg_postgres__orders') }} o ON e.order_id = o.order_id
LEFT JOIN
    {{ ref('postgres.stg_postgres__products') }} p ON e.product_id = p.product_id
ORDER BY
    e.session_id, e.created_at
