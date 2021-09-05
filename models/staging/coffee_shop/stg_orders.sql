with source as (
    select * from {{ source('coffee_shop', 'orders') }}
),

renamed as (
    select
        id 
        , customer_id
        , created_at 
        , total
        , address as shippig_address
        , state as shipping_state
        , zip as shipping_zip
    from source
)

select * from renamed