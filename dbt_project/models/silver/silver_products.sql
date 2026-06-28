{{ config(
    materialized='table'
) }}

with source_data as (
    select * from {{ source('datalake_source', 'bronze_products') }}

),

casted_and_cleaned as (
    select
        cast(trim(product_id) as varchar(50)) as product_id,
        trim(product_name) as product_name,
        
        case 
            when category is null then null
            else trim(category)
        end as category,

        case 
            when cast(unit_price as numeric(18,2)) <= 0 then null
            else cast(unit_price as numeric(18,2))
        end as unit_price,

        upper(trim(currency)) as currency_code,
        
        cast(_ingested_at as timestamp) as bronze_ingested_at,
        NOW() as _ingested_silver

    from source_data
),

deduplicated as (
    select 
        *,
        row_number() over (
            partition by product_id 
            order by case when upper(product_name) = 'DUPLICATE ROW' then 2 else 1 end asc,
                     bronze_ingested_at desc
        ) as row_num
    from casted_and_cleaned
)

select
    product_id,
    product_name,
    category,
    unit_price,
    currency_code,
    _ingested_silver,
    bronze_ingested_at
from deduplicated
where row_num = 1 and product_id is not null