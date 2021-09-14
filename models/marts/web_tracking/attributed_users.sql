{{ config(
    materialized='table'
    ) 
}}

-- 1. User Stitching
with 

user_stitching as (

    select 
        pageview_id
        , visitor_id
        , customer_id
        , case 
            when customer_id is not null then first_value(visitor_id) over (partition by customer_id order by web_tracking.timestamp) 
            else visitor_id
        end as attributed_user_id
        , device_type
        , timestamp
        , page_path
    from {{ ref('coffee_shop', 'stg_web_tracking') }} as web_tracking
),

-- 2. Sessionization 

time_between_pageviews as (
    select
        *
        , lag(timestamp) over (PARTITION BY attributed_user_id order by timestamp) as previous_page_view
        , date_diff(timestamp, lag(timestamp) over (PARTITION BY attributed_user_id order by timestamp), minute) as inactivity_time 
    from user_stitching
),

session_sequence as (
    select 
        attributed_user_id || '-' || row_number() over(partition by attributed_user_id order by time_between_pageviews.timestamp) as session_id
        , attributed_user_id
        , time_between_pageviews.timestamp as session_start_at
        , lead(time_between_pageviews.timestamp) over(partition by attributed_user_id order by time_between_pageviews.timestamp) as next_session_start_at
    from time_between_pageviews
    where inactivity_time > 30 or inactivity_time is null 
),

sessionized as (
    select
     user_stitching.pageview_id
    , user_stitching.attributed_user_id
    , user_stitching.timestamp
    , user_stitching.customer_id
    , user_stitching.visitor_id
    , user_stitching.device_type
    , user_stitching.page_path
    , session_sequence.session_id
    , session_sequence.session_start_at
    , session_sequence.next_session_start_at
    from session_sequence
    left join user_stitching on user_stitching.attributed_user_id = session_sequence.attributed_user_id
    and user_stitching.timestamp >= session_sequence.session_start_at
    and (user_stitching.timestamp < session_sequence.next_session_start_at or session_sequence.next_session_start_at is null) 

)

select * from sessionized
