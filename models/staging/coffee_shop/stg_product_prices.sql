with source as (
    select * from {{ source('coffee_shop', 'product_prices') }}
),

renamed as (
    select
        id as prices_id 
        , product_id
        , price as product_price
        , created_at 
        , ended_at
    from source
)

select * from renamed