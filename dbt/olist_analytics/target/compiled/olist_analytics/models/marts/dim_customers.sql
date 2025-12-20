with customers as (
    select * from `stellar-verve-478012-n6`.`olist_raw_staging`.`stg_customers`
)

select
    customer_id,        -- Included as requested (Primary Key)
    customer_unique_id, -- The actual person's ID (Foreign Key to user history)
    zip_code,
    city,
    state
from customers