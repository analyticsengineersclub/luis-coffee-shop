with source as (
    select * from {{ source('coffee_shop', 'products') }}
),

renamed as (
    select
        id 
        , name as product_name
        , category as product_category
        , created_at as product_created_at
    from source
)

select * from renamed
