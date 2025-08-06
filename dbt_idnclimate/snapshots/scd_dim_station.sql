{% snapshot scd_dim_station %}

{{
   config(
       target_schema='DEV',
       unique_key='station_id',
       strategy='timestamp',
       updated_at='updated_at',
       invalidate_hard_deletes=True
   )
}}

select * FROM {{ ref('dim_station') }}

{% endsnapshot %}