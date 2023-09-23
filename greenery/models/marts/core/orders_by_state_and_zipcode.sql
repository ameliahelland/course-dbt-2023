{{
    config(
        materialized='table'
    )
}}

SELECT
    state,
    zipcode,
    COUNT(*) AS OrderCount
FROM
    {{ source('postgres', 'orders') }} AS o
JOIN
    {{ source('postgres', 'addresses') }} AS a
ON
    o.address_id = a.address_id
GROUP BY
    state,
    zipcode
ORDER BY
    OrderCount DESC