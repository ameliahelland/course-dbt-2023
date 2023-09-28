{{
    config(
        materialized = 'table'
    )
}}

SELECT
    pios.product_name,
    inventory_used,
    inventory,
    average_amount_purchased_per_order,
    pageviews,
    revenue,
    total_sessions_added_to_cart,
    total_sessions_purchased,
    conversion_rate
FROM
    {{ ref('int__product_inventory_and_order_statistics') }} AS pios
LEFT JOIN
    {{ ref('int__product_pageviews') }} AS pp
ON
    pios.product_name = pp.product_name
LEFT JOIN
    {{ ref('int__product_revenues') }} AS pr
ON
    pios.product_name = pr.product_name
LEFT JOIN
    {{ ref('int__product_conversions')}} AS pc
ON
    pp.product_name = pc.product_name