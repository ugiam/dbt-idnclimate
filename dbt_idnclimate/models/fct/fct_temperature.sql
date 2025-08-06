{{
  config(
    materialized = 'incremental',
    on_schema_change='fail'
    )
}}

WITH climate_data AS (
  SELECT
    DATE,
    STATION_ID,
    AVG_TEMPERATURE,
    CASE
      WHEN MIN_TEMPERATURE > MAX_TEMPERATURE THEN MAX_TEMPERATURE
      ELSE MIN_TEMPERATURE
    END AS MIN_TEMPERATURE,
    CASE
      WHEN MIN_TEMPERATURE < MAX_TEMPERATURE THEN MAX_TEMPERATURE
      ELSE MIN_TEMPERATURE
    END AS MAX_TEMPERATURE,
    TO_CHAR(DATE, 'MM-DD') AS month_day
  FROM {{ ref('src_climate_data')}}
),
day_avg_tmp AS (
  SELECT
    month_day,
    STATION_ID as id,
    avg(AVG_TEMPERATURE) as AVG_TEMPERATURE
  FROM climate_data
  WHERE AVG_TEMPERATURE IS NOT NULL
  GROUP BY month_day, STATION_ID
),
day_min_tmp AS (
  SELECT
    month_day,
    STATION_ID as id,
    avg(MIN_TEMPERATURE) as MIN_TEMPERATURE
  FROM climate_data
  WHERE MIN_TEMPERATURE IS NOT NULL
  GROUP BY month_day, STATION_ID
),
day_max_tmp AS (
  SELECT
    month_day,
    STATION_ID as id,
    avg(MAX_TEMPERATURE) as MAX_TEMPERATURE
  FROM climate_data
  WHERE MAX_TEMPERATURE IS NOT NULL
  GROUP BY month_day, STATION_ID
)

SELECT
  b.DATE as DATE,
  b.STATION_ID,
  COALESCE(b.MIN_TEMPERATURE, mi.MIN_TEMPERATURE)::decimal(10,1) AS MIN_TEMPERATURE,
  COALESCE(b.MAX_TEMPERATURE, ma.MAX_TEMPERATURE)::decimal(10,1) AS MAX_TEMPERATURE,
  COALESCE(b.AVG_TEMPERATURE, av.AVG_TEMPERATURE)::decimal(10,1) AS AVG_TEMPERATURE
FROM climate_data b
LEFT JOIN day_avg_tmp av ON b.month_day = av.month_day and b.STATION_ID = av.id
LEFT JOIN day_min_tmp mi ON b.month_day = mi.month_day and b.STATION_ID = mi.id
LEFT JOIN day_max_tmp ma ON b.month_day = ma.month_day and b.STATION_ID = ma.id
WHERE
  mi.MIN_TEMPERATURE IS NOT NULL AND
  ma.MAX_TEMPERATURE IS NOT NULL AND
  av.AVG_TEMPERATURE IS NOT NULL

{% if is_incremental() %}
  AND DATE > (select max(DATE) from {{ this }})
{% endif %}