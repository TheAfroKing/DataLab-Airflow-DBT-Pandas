

SELECT
    dd.DateKey AS PaymentDateKey,
    dc.CustomerKey,
    p.checkNumber,
    p.amount

FROM "docker"."raw"."payments" AS p
LEFT JOIN "docker"."DWH"."dim_customer" AS dc
    ON p.customerNumber = dc.customerNumber
LEFT JOIN "docker"."DWH"."dim_date" AS dd
    -- ¡CAMBIO AQUÍ!
    ON CAST(p.paymentDate AS DATE) = CAST(dd.FullDate AS DATE)

WHERE dd.DateKey IS NOT NULL 
  AND dc.CustomerKey IS NOT NULL