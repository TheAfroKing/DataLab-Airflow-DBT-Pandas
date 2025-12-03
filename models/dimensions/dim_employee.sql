{{
  config(
    materialized='table'
  )
}}

SELECT
    e.employeeNumber AS EmployeeKey,
    o.OfficeKey, 
    e.employeeNumber,
    e.lastName,
    e.firstName,
    e.jobTitle,
    e.email,
    o.officeCode,
    e.reportsTo 
FROM {{ source('raw', 'employees') }} AS e
LEFT JOIN {{ ref('dim_office') }} AS o
    ON e.officeCode = o.officeCode