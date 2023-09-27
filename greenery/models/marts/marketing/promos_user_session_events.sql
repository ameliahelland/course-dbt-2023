{{
    config(
        materialized='table'
    )
}}


SELECT
    e.user_id AS UserId,
    e.session_id AS SessionId,
    e.event_id AS EventId,
    e.created_at AS EventCreatedAt,
    o.order_id AS OrderId,
    o.created_at AS OrderCreatedAt,
    o.promo_id AS PromoId
FROM
    {{ ref('stg_postgres__events') }} AS e
LEFT JOIN
    {{ ref('stg_postgres__orders') }} AS o
ON
    e.order_id = o.order_id
WHERE
    o.promo_id IS NOT NULL