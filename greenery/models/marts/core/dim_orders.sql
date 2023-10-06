{{
    config(
        materialized='table'
    )
}}

SELECT
    e.session_id,
    MAX(CASE WHEN e.event_type = 'add_to_cart' THEN e.product_id END) AS product_id,
    MAX(CASE WHEN e.event_type = 'add_to_cart' THEN e.event_id END) AS cart_event_id,
    MAX(CASE WHEN e.event_type = 'checkout' THEN e.event_id END) AS checkout_event_id,
    MAX(CASE WHEN e.event_type = 'checkout' THEN e.order_id END) AS order_id
FROM
    {{ ref('stg_postgres__events') }} e
WHERE
    e.event_type IN ('add_to_cart', 'checkout')
GROUP BY
    e.session_id