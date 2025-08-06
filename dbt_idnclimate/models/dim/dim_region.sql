{{
  config(
    materialized = 'view',
    )
}}

WITH src_region AS (
    SELECT
        *
    FROM
        {{ ref('src_region') }}
)

SELECT 
    *
FROM src_region