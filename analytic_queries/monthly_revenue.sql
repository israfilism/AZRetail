select 
    date_trunc('month', o.order_ts)::date  order_month,
    p.category,
    c.country_code,
    sum(o.quantity * o.unit_price_azn)  total_revenue_azn,
    count(distinct o.order_id)  total_orders
from datalake.gold_fct_order_items o
left join datalake.gold_dim_product p on o.product_id = p.product_id and p.is_current = true --and o.order_ts >= p.valid_from and o.order_ts < p.valid_to
left join datalake.gold_dim_customer c on o.customer_id = c.customer_id and c.is_current = true --and o.order_ts >= c.valid_from and o.order_ts < c.valid_to
group by 1, 2, 3
order by order_month desc, total_revenue_azn desc;