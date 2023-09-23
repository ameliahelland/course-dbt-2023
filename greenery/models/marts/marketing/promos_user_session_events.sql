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
        {{ ref('postgres.stg_postgres__events') }} AS e
    LEFT JOIN
        {{ ref('postgres.stg_postgres__orders') }} AS o
    ON
        e.order_id = o.order_id
    WHERE
        o.promo_id IS NOT NULL
)
SELECT
    pse.user_id,
    pse.session_id,
    pse.event_id,
    pse.event_created_at,
    pse.order_id,
    pse.order_created_at,
    CASE
        WHEN pse.order_id IS NOT NULL THEN 1
        ELSE 0
    END AS promo_order_flag
FROM
    PromoSessionEvents AS pse