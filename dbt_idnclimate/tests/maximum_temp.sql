SELECT
    *
FROM
    {{ ref('fct_temperature') }}
WHERE MIN_TEMPERATURE > MAX_TEMPERATURE
LIMIT 10
