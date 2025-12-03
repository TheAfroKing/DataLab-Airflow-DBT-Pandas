{% set start_date_sql = "(SELECT min_date FROM " ~ ref('stg_date_range') ~ ")" %}
{% set end_date_sql = "(SELECT max_date FROM " ~ ref('stg_date_range') ~ ")" %}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date = start_date_sql,
        end_date = end_date_sql
    ) }}
)

SELECT
    CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INT) AS DateKey,
    date_day AS FullDate,
    TRIM(TO_CHAR(date_day, 'Day')) AS DayName,
    DAYOFWEEK(date_day) AS DayOfWeek,
    DAY(date_day) AS DayOfMonth,
    TRIM(TO_CHAR(date_day, 'Month')) AS MonthName,
    MONTH(date_day) AS MonthOfYear,
    QUARTER(date_day) AS Quarter,
    YEAR(date_day) AS Year,
    (CASE 
        WHEN DAYOFWEEK(date_day) IN (1, 7) THEN TRUE
        ELSE FALSE
     END) AS IsWeekend
FROM date_spine