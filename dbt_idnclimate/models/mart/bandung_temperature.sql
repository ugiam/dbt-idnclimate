{{ 
    config(materialized = 'table') 
}}

WITH bandung_temperature as (
    SELECT
     * 
    FROM 
     {{ ref('fct_temperature') }}
),
station as(
    SELECT
     *
    FROM
     {{ ref("dim_station") }}
)

SELECT
 b.DATE AS DATE,
 s.STATION_NAME AS STATION_NAME,
 b.MAX_TEMPERATURE::decimal(10,1) AS MAX_TEMPERATURE,
 b.MIN_TEMPERATURE::decimal(10,1) AS MIN_TEMPERATURE,
 b.AVG_TEMPERATURE::decimal(10,1) AS AVG_TEMPERATURE
FROM
 bandung_temperature b
JOIN
 station s ON b.STATION_ID = s.STATION_ID
WHERE
 s.region_id = 177
 