{{
  config(
    materialized='table' 
  )
}}

SELECT
    dd.DateKey AS PaymentDateKey,
    dc.CustomerKey,
    p.checkNumber,
    p.amount

FROM {{ source('raw', 'payments') }} AS p
LEFT JOIN {{ ref('dim_customer') }} AS dc
    ON p.customerNumber = dc.customerNumber
LEFT JOIN {{ ref('dim_date') }} AS dd
    -- ¡CAMBIO AQUÍ!
    ON CAST(p.paymentDate AS DATE) = CAST(dd.FullDate AS DATE)

WHERE dd.DateKey IS NOT NULL 
  AND dc.CustomerKey IS NOT NULL