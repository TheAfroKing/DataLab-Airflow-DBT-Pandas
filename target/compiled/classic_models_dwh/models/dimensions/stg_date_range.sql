
SELECT
    MIN(orderDate) AS min_date,
    MAX(orderDate) AS max_date
FROM "docker"."raw"."orders"