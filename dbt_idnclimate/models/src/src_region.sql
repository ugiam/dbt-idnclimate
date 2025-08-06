WITH raw_region AS (
    SELECT
        *
    FROM
        {{ source('idnclimate', 'region') }}
)
SELECT 
    id as region_id,
    name as region_name,
    province_id
FROM
    raw_region