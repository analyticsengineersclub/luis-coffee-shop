{{ config(
    materialized='table',
    sort='month'
    ) 
}}

WITH monthly_first_orders AS (
    SELECT 
        DATE_TRUNC(customers.first_order, month) AS month
        , COUNT( customers.customer_id) AS first_time_buyers
    FROM {{ ref('customers') }} AS customers
    GROUP BY 1
) 

SELECT * FROM {{ source('coffee_shop', 'monthly_first_orders') }} ORDER BY 1 DESC
