{{
    config(
        materialized='table'
    )
}}

SELECT
    e.session_id AS SessionId,
    e.page_url AS PageURL,
    e.event_type AS EventType,
    e.created_at AS EventTimestamp,
    e.user_id AS UserId,
    u.first_name AS FirstName,
    u.last_name AS LastName,
    u.email AS Email,
    e.order_id AS OrderId,
    o.order_cost AS OrderCost,
    o.shipping_cost AS ShippingCost,
    o.order_total AS OrderTotal,
    o.tracking_id AS TrackingId,
    o.shipping_service AS ShippingService,
    o.estimated_delivery_at AS EstimatedDeliveryAt,
    o.delivered_at AS DeliveredAt
FROM
    {{ source('postgres', 'events') }} e
LEFT JOIN
    {{ source('postgres', 'users') }} u ON e.user_id = u.user_id
LEFT JOIN
    {{ source('postgres', 'orders') }} o ON e.order_id = o.order_id
ORDER BY
    e.session_id, e.created_at
