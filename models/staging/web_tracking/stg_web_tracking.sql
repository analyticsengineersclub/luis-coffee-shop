with source as (

    select * from {{ source('web_tracking', 'pageviews') }}

),

renamed as (

    select
        id as pageview_id
        , visitor_id
        , customer_id
        , device_type
        , page as page_path 
        , timestamp

    from source

)

select * from renamed