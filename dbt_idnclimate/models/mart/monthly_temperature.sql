{{ 
    config(materialized = 'table') 
}}

WITH climate_data as(
  SELECT
    TO_CHAR(DATE, 'YYYY-MM') AS MONTH,
    STATION_ID,
    avg(MIN_TEMPERATURE)::decimal(10,1) as MIN_TEMPERATURE,
    avg(MAX_TEMPERATURE)::decimal(10,1) as MAX_TEMPERATURE,
    avg(AVG_TEMPERATURE)::decimal(10,1) as AVG_TEMPERATURE
  FROM {{ ref('fct_temperature') }}
  GROUP BY 
    MONTH, STATION_ID
  ORDER BY 
    MONTH, STATION_ID
)

SELECT
  *
FROM 
  climate_data