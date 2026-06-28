{{ config(
    materialized='table'

) }}

with source_data as (
    select * from {{ source('datalake_source', 'bronze_customers') }}

),

casted_and_cleaned as (
    select        
        cast(trim(customer_id) as varchar(50)) as customer_id,
        trim(full_name) as customer_name,
        
        case 
            when lower(trim(country)) in ('azerbaijan', 'az') then 'AZ'
            when lower(trim(country)) in ('turkey', 'tr') then 'TR'
            when lower(trim(country)) in ('russia', 'ru') then 'RU'
            when lower(trim(country)) in ('united states', 'usa', 'us') then 'US'
            when lower(trim(country)) in ('germany', 'de') then 'DE'
            when lower(trim(country)) in ('georgia', 'ge') then 'GE'
            else upper(trim(country))
        end as country_code,
        
        case 
            when segment is null then null
            else upper(trim(segment))
        end as customer_segment,
        
        case 
            when signup_date like '%.%.%' then to_date(signup_date, 'DD.MM.YYYY')::timestamp
            else cast(signup_date as timestamp)
        end as signup_date_ts,

        cast(_ingested_at as timestamp) as bronze_ingested_at,
        NOW() as _ingested_silver
    from source_data
),

deduplicated as (
    select 
        *,
        row_number() over (
            partition by customer_id 
            order by signup_date_ts desc
        ) as row_num
    from casted_and_cleaned
)

select
    customer_id,
    customer_name,
    country_code,
    customer_segment,
    signup_date_ts,
    _ingested_silver,
    bronze_ingested_at
from deduplicated
where row_num = 1 AND customer_id is not null