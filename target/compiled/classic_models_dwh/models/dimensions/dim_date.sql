


WITH date_spine AS (
    





with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
    
    

    )

    select *
    from unioned
    where generated_number <= 337
    order by generated_number



),

all_periods as (

    select (
        

    timestampadd(
        day,
        row_number() over (order by 1) - 1,
        (SELECT min_date FROM "docker"."DWH"."stg_date_range")
        )


    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= (SELECT max_date FROM "docker"."DWH"."stg_date_range")

)

select * from filtered


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