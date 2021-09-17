{{ config(
    materialized='table'
    ) 
}}

with 

customers as (select * from {{ ref('coffee_shop', 'stg_customers') }}),

orders as (select * from {{ ref('coffee_shop', 'stg_orders') }}),

customer_orders as (
    select 
        customer_id
        , count(*) as number_of_orders
        , min(created_at) as first_order_at
        , sum(total) as total_order_value
    from orders
    group by 1
),

final as (
    select 
        customers.id as customer_id
        , customers.customer_name 
        , customers.customer_email 
        , customer_orders.number_of_orders
        , customer_orders.first_order_at
        , customer_orders.total_order_value
    from
        customers
    join customer_orders 
        on customers.id = customer_orders.customer_id
)

SELECT * from final
