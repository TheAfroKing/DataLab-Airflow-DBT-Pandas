

SELECT
    dd_order.DateKey AS OrderDateKey,
    dd_shipped.DateKey AS ShippedDateKey,
    dp.ProductKey,
    dc.CustomerKey,
    de.EmployeeKey,
    de.OfficeKey,
    o.orderNumber,
    od.orderLineNumber,
    o.status AS orderStatus,
    od.quantityOrdered,
    od.priceEach,
    (od.quantityOrdered * od.priceEach) AS totalLineAmount,
    ((od.priceEach - dp.buyPrice) * od.quantityOrdered) AS profit
FROM "docker"."raw"."orders" AS o
JOIN "docker"."raw"."orderdetails" AS od
    ON o.orderNumber = od.orderNumber
LEFT JOIN "docker"."DWH"."dim_product" AS dp
    ON od.productCode = dp.productCode
LEFT JOIN "docker"."DWH"."dim_customer" AS dc
    ON o.customerNumber = dc.customerNumber
LEFT JOIN "docker"."DWH"."dim_employee" AS de
    ON dc.SalesRepEmployeeKey = de.EmployeeKey
LEFT JOIN "docker"."DWH"."dim_date" AS dd_order
    -- ¡CAMBIO AQUÍ!
    ON CAST(o.orderDate AS DATE) = CAST(dd_order.FullDate AS DATE)
LEFT JOIN "docker"."DWH"."dim_date" AS dd_shipped
    -- ¡CAMBIO AQUÍ!
    ON CAST(o.shippedDate AS DATE) = CAST(dd_shipped.FullDate AS DATE)

