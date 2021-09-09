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
    orders.order_id
    , orders.created_at
    , case when first_value(orders.created_at) OVER (PARTITION BY customer_id order by orders.created_at) = orders.created_at then 'new customer' 
      else 'existing customer' 
      end AS is_new_customer
    , products.product_category
    , product_prices.product_price
       
    from order_items

    left join orders
    on orders.order_id = order_items.order_id

    left join products 
    on products.id = order_items.product_id

    left join product_prices 
    on product_prices.product_id = products.id
    and orders.created_at between product_prices.created_at and product_prices.ended_at

)

 select * from final 
