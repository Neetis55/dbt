select 
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,

    {{ cents_to_dollars('amount', 4) }} amount,
    created as created_at

from "RAW"."STRIPE"."PAYMENT"