{{ config(materialized='view') }}
SELECT
    MIN(orderDate) AS min_date,
    MAX(orderDate) AS max_date
FROM {{ source('raw', 'orders') }}

