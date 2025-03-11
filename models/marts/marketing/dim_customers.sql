with customers as (
    select *
    from {{ ref('stg_jaffle_shop__customers') }}
),

orders as (
    select *
    from {{ ref('stg_jaffle_shop__orders') }}
),

customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from orders
    group by customer_id
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    co.first_order_date,
    co.most_recent_order_date,
    ifnull(co.number_of_orders, 0) as number_of_orders,
    (
        select sum(amount)
        from {{ ref('fct_orders') }} o
        where c.customer_id = o.customer_id
    ) as lifetime_value
from customers c
left join customer_orders co
    using (customer_id)