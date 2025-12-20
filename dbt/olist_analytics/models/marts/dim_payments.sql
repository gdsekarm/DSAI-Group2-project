with payments as (
    select * from {{ ref('stg_order_payments') }}
)

select
    -- Generate a unique key for each payment row
    concat(order_id, '-', cast(payment_sequential as string)) as payment_sk,
    
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
from payments