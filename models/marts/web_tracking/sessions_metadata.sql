{{ config(
    materialized='table'
    ) 
}}

with 

sessions_purchase as (
    select 
    session_id
    , page_path
    , (case 
        when page_path = 'order-confirmation' then true
        else false 
        end
    ) as purchase_made
    from 
        {{ ref('coffee_shop', 'attributed_users') }}
    where 
    1=1
    and page_path = 'order-confirmation'),  

sessions_aggregated as (
    select
    session_id
    , count(distinct pageview_id) as pages_visited
    , min(timestamp) as session_start_at
    , max(timestamp) as session_ended_at
    from 
        {{ ref('coffee_shop', 'attributed_users') }}
    group by 
        1
    ),

final as (
    select 
        sessions_aggregated.session_id
        , sessions_aggregated.pages_visited 
        , date_diff(session_ended_at, session_start_at, minute) as session_minutes
        , coalesce(sessions_purchase.purchase_made,false) as purchase_made
    from 
        sessions_aggregated
    left join 
        sessions_purchase
    on 
        sessions_purchase.session_id = sessions_aggregated.session_id
)

select * from final
