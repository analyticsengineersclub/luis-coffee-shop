{{ config(materialized='table') }}

WITH 

customer_orders AS (
    SELECT 
        customer_id
        , COUNT(*) AS number_of_orders
        , MIN(created_at) AS first_order
        , SUM(total) AS total_order_value
    FROM 
        `analytics-engineers-club.coffee_shop.orders`
    GROUP BY 1
),

final AS (
    SELECT 
        customers.id AS customer_id
        , customers.name AS customer_name
        , customers.email AS customer_email
        , customer_orders.number_of_orders
        , customer_orders.first_order
        , customer_orders.total_order_value
    FROM
        `analytics-engineers-club.coffee_shop.customers` AS customers
    JOIN customer_orders 
        ON customers.id = customer_orders.customer_id
)

SELECT * FROM final
