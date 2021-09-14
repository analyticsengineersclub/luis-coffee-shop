{{ config(
    materialized='table'
    ) 
}}

WITH

product_prices AS ( select * from {{ ref('coffee_shop', 'stg_product_prices') }}),

products AS ( select * from {{ ref('coffee_shop', 'stg_products') }}),

order_items AS ( select * from {{ ref('coffee_shop', 'stg_order_items') }}), 

orders AS ( select * from {{ ref('coffee_shop', 'stg_orders') }}),

final as (
    select 
     order_items.id as order_items_id
    , orders.order_id as order_id
    , orders.created_at
    , products.product_category
    , product_prices.product_price
       
    from order_items

    left join orders
        on order_items.order_id = orders.order_id

    left join products 
        on order_items.product_id = products.id

    left join product_prices 
        on order_items.product_id = product_prices.product_id
        and orders.created_at between product_prices.created_at and product_prices.ended_at

)

 select * from final 
