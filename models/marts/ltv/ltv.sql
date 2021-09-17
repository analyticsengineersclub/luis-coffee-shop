with

all_weeks as ( 
    select * from {{ ref('all_weeks') }}
),

orders as ( 
    select * from {{ ref('coffee_shop', 'orders') }}
),

customers_weekly_orders as ( 
    select
    customer_id
    , date_trunc(created_at, week) as date_week
    , sum(total) as total_revenue
    from orders
    group by 1, 2
), 

customers_first_week as (
    select 
    customer_id
    , min(date_week) as first_order_week_at
    from customers_weekly_orders
    group by 1
), 

date_spined as (
    select
        customers_first_week.customer_id as customer_id
        , date_trunc(all_weeks.date_week, week) as date_week
        , date_diff(
            cast(all_weeks.date_week as datetime),
            cast(customers_first_week.first_order_week_at as datetime),
            week
        ) as week_number
    from all_weeks
    inner join customers_first_week
        on all_weeks.date_week >= customers_first_week.first_order_week_at 
),

week_customers_joined as (
    select
    date_spined.customer_id
    , date_spined.date_week
    , date_spined.week_number as week_number 
    , coalesce(customers_weekly_orders.total_revenue, 0) as weekly_total_revenue
    
    , sum(coalesce(customers_weekly_orders.total_revenue, 0)) over (
        partition by date_spined.customer_id 
        order by date_spined.week_number
        rows between unbounded preceding and current row
    ) as weekly_cumulative_revenue

    from date_spined
    left join customers_weekly_orders
    on date_spined.customer_id = customers_weekly_orders.customer_id
    and date_spined.date_week = customers_weekly_orders.date_week
)

select * from week_customers_joined 
