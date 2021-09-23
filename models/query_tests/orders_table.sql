{{ config(
    materialized='table'
    ) 
}}

with 

orders_monthly_count as (

    select 
    count(distinct id) as number_of_orders
    , date_trunc(created_at, month) as created_at_month
    from {{ source('coffee_shop', 'orders') }}
    group by 2
    order by 2 desc
    
)

select * from orders_monthly_count