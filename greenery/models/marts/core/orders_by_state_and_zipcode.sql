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
    {{ source('postgres', 'stg_postgres__orders') }} AS o
JOIN
    {{ source('postgres', 'stg_postgres__addresses') }} AS a
ON
    o.address_id = a.address_id
GROUP BY
    state,
    zipcode
ORDER BY
    OrderCount DESC