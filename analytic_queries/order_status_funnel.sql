with status_counts as (
    select 
        order_status,
        count(distinct order_id)  unique_orders
    from datalake.gold_fct_order_items
    group by 1
),
totals as (
    select count(distinct order_id)  grand_total from datalake.gold_fct_order_items
)
select 
    s.order_status,
    s.unique_orders,
    t.grand_total,
    round((s.unique_orders::numeric / nullif(t.grand_total, 0)) * 100, 2)  rate_pct
from status_counts s
cross join totals t
order by s.unique_orders desc;
