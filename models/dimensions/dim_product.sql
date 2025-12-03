{{
  config(
    materialized='table'
  )
}}

SELECT
    p.productCode AS ProductKey, 
    p.productCode,
    p.productName,
    p.productScale,
    p.productVendor,
    p.buyPrice,
    p.MSRP,
    p.productLine,
    pl.textDescription
FROM {{ source('raw', 'products') }} AS p
LEFT JOIN {{ source('raw', 'productlines') }} AS pl
    ON p.productLine = pl.productLine