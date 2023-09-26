{{
    config(
        materialized='table'
    )
}}

WITH PromoSessionEvents AS (
    SELECT
        e.user_id,
        e.session_id,
        e.event_id,
        e.created_at AS event_created_at,
        o.order_id,
        o.created_at AS order_created_at
    FROM
        {{ ref('stg_postgres__events') }} AS e
    LEFT JOIN
        {{ ref('stg_postgres__orders') }} AS o
    ON
        e.order_id = o.order_id
    WHERE
        o.promo_id IS NOT NULL
)
SELECT
    pse.user_id AS UserId,
    pse.session_id AS SessionId,
    pse.event_id AS EventId,
    pse.event_created_at AS EventCreatedAt,
    pse.order_id AS OrderId,
    pse.order_created_at AS OrderCreatedAt,
    CASE
        WHEN pse.order_id IS NOT NULL THEN 1
        ELSE 0
    END AS promo_order_flag
FROM
    PromoSessionEvents AS pse