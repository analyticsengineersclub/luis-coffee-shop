{{ config(
    materialized='table'
    ) 
}}

WITH

product_prices AS ( select * from {{ source('coffee_shop', 'product_prices') }}),

products AS ( select * from {{ source('coffee_shop', 'products') }}),

order_items AS ( select * from {{ source('coffee_shop', 'order_items') }}), 

orders AS ( select * from {{ source('coffee_shop', 'orders') }}),

final as (
    select 
    orders.created_at
    , case when first_value(orders.created_at) OVER (PARTITION BY customer_id order by orders.created_at) = orders.created_at then 'new customer' 
      else 'existing customer' 
      end AS is_new_customer
    , products.category
    , product_prices.price
       
    from order_items

    left join orders
    on order_items.id = orders.id 

    left join products 
    on products.id = order_items.product_id

    left join product_prices 
    on product_prices.product_id = products.id
 --   and orders.created_at between product_prices.created_at and product_prices.ended_at

)

 select * from final 
