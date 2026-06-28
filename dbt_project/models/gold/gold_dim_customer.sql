{{ config(materialized='table') }}

select
    md5(cast(concat(customer_id, '-', dbt_valid_from) as varchar)) as customer_sk,
    customer_id,
    customer_name,
    country_code,
    customer_segment,
    
    dbt_valid_from as valid_from,
    coalesce(dbt_valid_to, '9999-12-31'::timestamp) as valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref('snap_customers') }}