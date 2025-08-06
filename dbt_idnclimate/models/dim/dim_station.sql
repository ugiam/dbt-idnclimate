{{
  config(
    materialized = 'view',
    )
}}

WITH 
src_station AS (
    SELECT
        *
    FROM
        {{ ref('src_station') }}
),
src_climate_data as (
    SELECT
        MIN(date) as date,
        station_id
    FROM
        {{ ref('src_climate_data')}}
    GROUP BY
        station_id
)

SELECT 
    s.station_ID as station_id,
	s.station_name as station_name,
	s.region_id as region_id,
	s.lattitude as lattitude,
	s.longitude as longitude,
    c.date AS CREATED_AT,
    c.date AS UPDATED_AT
FROM src_station s
LEFT JOIN src_climate_data c ON s.station_id = c.station_id
WHERE c.date IS NOT NULL