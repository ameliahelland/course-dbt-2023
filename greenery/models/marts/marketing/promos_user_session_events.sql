{{
    config(
        materialized='table'
    )
}}


SELECT
    e.user_id,
    e.session_id,
    e.event_id,
    e.created_at AS event_created_at,
    o.order_id,
    o.created_at AS order_created_at,
    o.promo_id
FROM
    {{ ref('stg_postgres__events') }} AS e
LEFT JOIN
    {{ ref('stg_postgres__orders') }} AS o
ON
    e.order_id = o.order_id
WHERE
    o.promo_id IS NOT NULL