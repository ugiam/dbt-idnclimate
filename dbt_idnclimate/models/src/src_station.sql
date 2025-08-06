WITH raw_station AS (
    SELECT
        *
    FROM
        {{ source('idnclimate', 'station') }}
)
SELECT
    id AS station_id,
    name AS station_name,
    region_id,
    lattitude,
    longitude
FROM
    raw_station
