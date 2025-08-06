{{
  config(
    materialized = 'view',
    )
}}

WITH src_province AS (
    SELECT
        *
    FROM
        {{ ref('src_province') }}
)

SELECT 
    *
FROM src_province