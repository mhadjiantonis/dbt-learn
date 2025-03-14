select
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status as payment_status,
    round(amount / 100, 2) as payment_amount,
    created as created_at
from {{ source('stripe', 'payment') }}