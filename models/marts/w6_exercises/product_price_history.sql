with 

product_price_history as (

    select *
    from {{ ref('stg_product_prices') }}
)

select * from product_price_history