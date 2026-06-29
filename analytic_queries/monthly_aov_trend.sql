with order_totals as (
    select 
        date_trunc('month', order_ts)::date  order_month,
        order_id,
        sum(quantity * unit_price_azn)  sum_amount_azn
    from datalake.gold_fct_order_items
    group by 1, 2
)
select 
    order_month,
    round(avg(sum_amount_azn), 2)  average_order_value_azn,
    round(sum(sum_amount_azn), 2)  total_monthly_revenue_azn
from order_totals
group by 1
order by order_month desc;