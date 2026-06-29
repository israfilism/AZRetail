select 
    o.customer_id,
    c.customer_name,
    c.country_code,
    sum(o.quantity * o.unit_price_azn) as total_spent_azn,
    count(distinct o.order_id) as orders_count
from datalake.gold_fct_order_items o
left join datalake.gold_dim_customer c on o.customer_id = c.customer_id and c.IS_CURRENT = TRUE
group by 1, 2, 3
order by total_spent_azn desc
limit 10;