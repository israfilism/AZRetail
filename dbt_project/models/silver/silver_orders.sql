{{ config(
    materialized='incremental',
    unique_key='order_item_key',
    incremental_strategy='merge'
) }}

with source_data as (
    select * from {{ source('datalake_source', 'bronze_orders') }}
    {% if is_incremental() %}
    where _ingested_at > (select max(bronze_ingested_at) from {{ this }})
    {% endif %}
),

casted_and_cleaned as (
    select
        md5(cast(concat(trim(order_id), '-', trim(product_id)) as varchar)) as order_item_key,
        
        cast(trim(order_id) as varchar(50)) as order_id,
        cast(trim(customer_id) as varchar(50)) as customer_id,
        cast(trim(product_id) as varchar(50)) as product_id,
        
        cast(replace(order_ts, 'T', ' ') as timestamp) as order_ts,
        
        upper(trim(status)) as order_status,
        upper(trim(currency)) as currency_code,
        
        cast(quantity as integer) as quantity,
        cast(unit_price as numeric(18,2)) as unit_price,
        
        cast(_ingested_at as timestamp) as bronze_ingested_at,
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
            partition by order_id, product_id 
            order by bronze_ingested_at desc
        ) as row_num
    from casted_and_cleaned
)

select
    order_item_key,
    order_id,
    customer_id,
    product_id,
    order_ts,
    order_status,
    currency_code,
    quantity,
    unit_price,
    bronze_ingested_at,
    _ingested_silver
from deduplicated
where row_num = 1