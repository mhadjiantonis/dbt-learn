select
    o.customer_id,
    p.order_id,
    sum(p.payment_amount) as payment_amount
from {{ ref('stg_stripe__payments') }} p
join {{ ref('stg_jaffle_shop__orders') }} o
    using (order_id)
where p.payment_status = 'success'
group by
    customer_id,
    order_id