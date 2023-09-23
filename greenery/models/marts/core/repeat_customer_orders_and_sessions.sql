{{
    config(
        materialized='table'
    )
}}

WITH RepeatOrderUsers AS (
    SELECT
        o.user_id,
        COUNT(DISTINCT o.order_id) AS TotalOrders,
        MAX(o.created_at) AS LatestOrderDate,
        MIN(o.created_at) AS FirstOrderDate,
        SUM(o.order_cost) AS TotalPayments
    FROM
        {{ ref('postgres.stg_postgres__orders') }} o
    GROUP BY
        o.user_id
    HAVING
        COUNT(DISTINCT o.order_id) > 1
),
RepeatOrderDetails AS (
    SELECT
        r.user_id,
        r.TotalOrders,
        r.FirstOrderDate,
        r.LatestOrderDate,
        r.TotalPayments,
        COUNT(DISTINCT e.event_id) AS TotalEvents,
        COUNT(DISTINCT e.product_id) AS UniqueProductsViewed,
        COUNT(DISTINCT e.session_id) AS TotalSessions -- Count of distinct sessions
    FROM
        RepeatOrderUsers r
    LEFT JOIN
        {{ ref('postgres.stg_postgres__events') }} e ON r.user_id = e.user_id
    GROUP BY
        r.user_id, r.TotalOrders, r.FirstOrderDate, r.LatestOrderDate, r.TotalPayments
)
SELECT
    rd.user_id,
    u.first_name AS UserFirstName,
    u.last_name AS UserLastName,
    u.email AS UserEmail,
    rd.TotalOrders,
    rd.TotalPayments,
    rd.FirstOrderDate,
    rd.LatestOrderDate,
    rd.TotalEvents,
    rd.TotalSessions, -- Include the count of distinct sessions
    rd.UniqueProductsViewed
FROM
    RepeatOrderDetails rd
LEFT JOIN
    {{ ref('postgres.stg_postgres__users') }} u ON rd.user_id = u.user_id
ORDER BY
    rd.TotalOrders DESC