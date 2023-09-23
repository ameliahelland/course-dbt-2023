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
        {{ source('postgres', 'orders') }} o
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
        {{ source('postgres', 'events') }} e ON r.user_id = e.user_id
    GROUP BY
        r.user_id, r.TotalOrders, r.FirstOrderDate, r.LatestOrderDate, r.TotalPayments
)
SELECT
    rd.user_id AS UserId,
    u.first_name AS FirstName,
    u.last_name AS LastName,
    u.email AS Email,
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
    {{ source('postgres', 'users') }} u ON rd.UserId = u.user_id
ORDER BY
    rd.TotalOrders DESC