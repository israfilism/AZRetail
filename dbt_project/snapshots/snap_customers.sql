{% snapshot snap_customers %}

{{
    config(
      target_schema='datalake',
      unique_key='customer_id',
      strategy='check',
      check_cols=['customer_name', 'country_code', 'customer_segment']
    )
}}

select 
    customer_id,
    customer_name,
    country_code,
    customer_segment
from {{ ref('silver_customers') }}

{% endsnapshot %}