{{
  config(
    materialized='table'
  )
}}

SELECT
    officeCode AS OfficeKey, 
    officeCode,
    city,
    phone,
    addressLine1,
    addressLine2,
    state,
    country,
    postalCode,
    territory
FROM {{ source('raw', 'offices') }}