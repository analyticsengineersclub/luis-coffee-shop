{{ config(
    materialized='table'
    ) 
}}

with  orders as (select * from {{ ref('stg_orders') }}
), 

final as (
    select 
        order_id
        , customer_id
        , created_at 

        , row_number() over (
            partition by customer_id
            order by created_at
        ) = 1 AS is_new_customer

        , total
        , shippig_address
        , shipping_state
        , shipping_zip_code

    from orders
)

select * from final