WITH raw_climate_data AS (
    SELECT
        *
    FROM
        {{ source('idnclimate', 'climate_data') }}
)
SELECT
    *
FROM
    raw_climate_data