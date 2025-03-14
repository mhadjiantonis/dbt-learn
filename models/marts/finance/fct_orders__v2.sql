with
    stg_customers as (select * from {{ ref("stg_jaffle_shop__customers") }}),
    stg_orders as (select * from {{ ref("stg_jaffle_shop__orders") }}),
    stg_payments as (select * from {{ ref("stg_stripe__payments") }}),
    order_payments as (
        select
            order_id,
            max(created_at) as payment_finalized_date,
            sum(payment_amount) as total_amount_paid
        from stg_payments
        where payment_status <> 'fail'
        group by order_id
    ),
    customer_orders as (
        select
            o.order_id,
            c.customer_id,
            o.order_placed_at,
            o.order_status,
            min(o.order_placed_at) over per_customer as first_order_date,
            max(o.order_placed_at) over per_customer as most_recent_order_date,
            count(o.order_id) over per_customer as number_of_orders,
            row_number() over up_to_date as transaction_seq,
            row_number() over per_customer_up_to_date as customer_sales_seq,
            op.total_amount_paid,
            op.payment_finalized_date,
            if(
                o.order_placed_at = min(o.order_placed_at) over per_customer,
                'new',
                'return'
            ) as is_new_return_customer,
            sum(op.total_amount_paid) over per_customer_up_to_date
            as customer_lifetime_value,
            c.customer_first_name,
            c.customer_last_name
        from stg_customers c
        join stg_orders o using (customer_id)
        left join order_payments op using (order_id)
        window
            per_customer as (partition by customer_id),
            up_to_date as (order by o.order_placed_at, o.order_id),
            per_customer_up_to_date as (
                partition by customer_id order by o.order_placed_at, o.order_id
            )
    )
select *
from customer_orders
order by order_id
