

SELECT
    c.customerNumber AS CustomerKey, 
    c.customerNumber,
    c.customerName,
    c.contactLastName,
    c.contactFirstName,
    c.phone,
    c.city,
    c.state,
    c.country,
    c.creditLimit,
    de.EmployeeKey AS SalesRepEmployeeKey
    
FROM "docker"."raw"."customers" AS c
LEFT JOIN "docker"."DWH"."dim_employee" AS de
    ON c.salesRepEmployeeNumber = de.employeeNumber