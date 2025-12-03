
  
  
  create view "docker"."DWH"."stg_date_range__dbt_tmp" include schema privileges as (
    
SELECT
    MIN(orderDate) AS min_date,
    MAX(orderDate) AS max_date
FROM "docker"."raw"."orders"
  );