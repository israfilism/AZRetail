{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_item_key',
    incremental_predicates=[
        "order_date = '" ~ var('execution_date') ~ "'"
    ]
) }}

with source_data as (
    select * from {{ source('datalake_source', 'bronze_orders') }}   
    where LEFT(order_ts, 10) = '{{ var("execution_date") }}'


),

casted_and_cleaned as (
    select
        md5(cast(concat(
            coalesce(trim(order_id), ''), '-',
            coalesce(trim(customer_id), ''), '-',
            coalesce(trim(order_ts), ''), '-',
            coalesce(trim(status), ''), '-',
            coalesce(trim(currency), ''), '-',
            coalesce(trim(product_id), ''), '-',
            coalesce(cast(quantity as varchar), ''), '-',
            coalesce(cast(unit_price as varchar), '')
        ) as varchar)) as order_item_key,
        
        cast(trim(order_id) as varchar(50)) as order_id,
        cast(trim(customer_id) as varchar(50)) as customer_id,
        cast(trim(product_id) as varchar(50)) as product_id,
        
        cast(replace(order_ts, 'T', ' ') as timestamp) as order_ts,
        cast(replace(order_ts, 'T', ' ') as date) as order_date,

        upper(trim(status)) as order_status,
        upper(trim(currency)) as currency_code,
        
        cast(quantity as integer) as quantity,
        cast(unit_price as numeric(18,2)) as unit_price,
        NOW() as _ingested_silver

    from source_data
    where quantity > 0 
      and unit_price > 0 
      and order_id is not null 
      and product_id is not null
      and customer_id is not null
      and upper(trim(currency)) is not null
),

deduplicated as (
    select 
        *,
        row_number() over (
            partition by order_item_key 
            order by order_ts desc
        ) as row_num
    from casted_and_cleaned
)

select
    order_item_key,
    order_id,
    customer_id,
    product_id,
    order_date,
    order_ts,
    order_status,
    currency_code,
    quantity,
    unit_price,
    _ingested_silver
from deduplicated
where row_num = 1