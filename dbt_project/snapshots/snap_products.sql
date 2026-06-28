{% snapshot snap_products %}

{{
    config(
      target_schema='datalake',
      unique_key='product_id',
      strategy='check',
      check_cols=['product_name', 'category', 'unit_price', 'currency_code']
    )
}}

select 
    product_id,
    product_name,
    category,
    unit_price,
    currency_code
from {{ ref('silver_products') }}

{% endsnapshot %}