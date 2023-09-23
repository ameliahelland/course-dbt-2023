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
    {{ ref('postgres.stg_postgres__orders') }} AS o
JOIN
    {{ ref('postgres.stg_postgres__addresses') }} AS a
ON
    o.address_id = a.address_id
GROUP BY
    state,
    zipcode
ORDER BY
    OrderCount DESC