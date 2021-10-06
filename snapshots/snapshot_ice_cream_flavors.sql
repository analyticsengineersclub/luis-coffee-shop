{% snapshot ice_cream_snapshot %}

{{ config(
      target_schema='dbt_luis_snapshot',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
) 

}}

select * from {{ source('advanced_dbt_examples', 'favorite_ice_cream_flavors') }}

{% endsnapshot %}