{{
    config(
        materialized='table'
    )
}}

WITH RepeatOrderUsers AS (
    SELECT
        o.user_id,
        COUNT(DISTINCT o.order_id) AS total_orders,
        MAX(o.created_at) AS latest_order_date,
        MIN(o.created_at) AS first_order_date,
        SUM(o.order_cost) AS total_payments
    FROM
        {{ ref('stg_postgres__orders') }} o
    GROUP BY
        o.user_id
    HAVING
        COUNT(DISTINCT o.order_id) > 1
),
RepeatOrderDetails AS (
    SELECT
        r.user_id,
        r.total_orders,
        r.first_order_date,
        r.latest_order_date,
        r.total_payments,
        COUNT(DISTINCT e.event_id) AS total_events,
        COUNT(DISTINCT e.product_id) AS unique_products_viewed,
        COUNT(DISTINCT e.session_id) AS total_sessions -- Count of distinct sessions
    FROM
        RepeatOrderUsers r
    LEFT JOIN
        {{ ref('stg_postgres__events') }} e ON r.user_id = e.user_id
    GROUP BY
        r.user_id, r.total_orders, r.first_order_date, r.latest_order_date, r.total_payments
)
SELECT
    rd.user_id,
    u.first_name,
    u.last_name,
    u.email,
    rd.total_orders,
    rd.total_payments,
    rd.first_order_date,
    rd.latest_order_date,
    rd.total_events,
    rd.total_sessions, -- Include the count of distinct sessions
    rd.unique_products_viewed
FROM
    RepeatOrderDetails rd
LEFT JOIN
    {{ ref('stg_postgres__users') }} u ON rd.user_id = u.user_id
ORDER BY
    rd.total_orders DESC