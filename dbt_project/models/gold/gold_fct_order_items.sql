{{ config(
    materialized='incremental',
    unique_key='order_item_key',
    incremental_strategy='merge'
) }}

with silver_orders as (
    select * from {{ ref('silver_orders') }}
    {% if is_incremental() %}
    where bronze_ingested_at > (select max(bronze_ingested_at) from {{ this }})
    {% endif %}
),

currency as (
    select 
        currency_code, 
        cast(rate_to_azn as numeric(18,6)) as rate_to_azn 
    from {{ source('datalake_source', 'bronze_currency') }}
)

select
    o.order_item_key,
    o.order_id,
    o.customer_id,
    o.product_id,
    cast(to_char(o.order_ts, 'YYYYMMDD') as integer) as order_date_id,
    o.order_ts,
    o.order_status,
    o.quantity,
    o.unit_price as original_unit_price,
    o.currency_code,
    case 
        when o.currency_code = 'AZN' then cast(o.unit_price as numeric(18,2))
        else cast((o.unit_price * c.rate_to_azn) as numeric(18,2))
    end as unit_price_azn,
    o.bronze_ingested_at,
    NOW() as _ingested_gold
from silver_orders o
left join currency c on o.currency_code = c.currency_code