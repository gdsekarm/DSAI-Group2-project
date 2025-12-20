

  create or replace view `stellar-verve-478012-n6`.`olist_raw_staging`.`stg_order_payments`
  OPTIONS()
  as with source as (
    select * from `stellar-verve-478012-n6`.`olist_raw`.`payments`
),

renamed as (
    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value
    from source
)

select * from renamed;

