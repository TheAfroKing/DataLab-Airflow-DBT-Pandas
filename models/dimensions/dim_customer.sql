{{
  config(
    materialized='table'
  )
}}

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
    
FROM {{ source('raw', 'customers') }} AS c
LEFT JOIN {{ ref('dim_employee') }} AS de
    ON c.salesRepEmployeeNumber = de.employeeNumber