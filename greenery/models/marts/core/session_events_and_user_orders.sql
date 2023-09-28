{{
    config(
        materialized='table'
    )
}}

SELECT
    e.session_id,
    e.page_url AS page_URL,
    e.event_type AS event_type,
    e.created_at AS event_timestamp,
    e.user_id,
    u.first_name,
    u.last_name,
    u.email,
    e.order_id,
    o.order_cost,
    o.shipping_cost,
    o.order_total,
    o.tracking_id,
    o.shipping_service,
    o.estimated_delivery_at,
    o.delivered_at
FROM
    {{ ref('stg_postgres__events') }} e
LEFT JOIN
    {{ ref('stg_postgres__users') }} u ON e.user_id = u.user_id
LEFT JOIN
    {{ ref('stg_postgres__orders') }} o ON e.order_id = o.order_id
ORDER BY
    e.created_at, e.session_id
