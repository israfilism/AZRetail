{{ config(materialized='table') }}

select
    md5(cast(concat(product_id, '-', dbt_valid_from) as varchar)) as product_sk,
    product_id,
    product_name,
    category,
    unit_price,
    currency_code,

    dbt_valid_from as valid_from,
    coalesce(dbt_valid_to, '9999-12-31'::timestamp) as valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref('snap_products') }}