WITH raw_province AS (
    SELECT
        *
    FROM
        {{ source('idnclimate', 'province') }}
)
SELECT
    id AS province_id,
    name AS province_name,
FROM
    raw_province
