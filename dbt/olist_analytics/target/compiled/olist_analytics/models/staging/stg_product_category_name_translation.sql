with source as (
    select * from `stellar-verve-478012-n6`.`olist_raw`.`category_translation`
),

renamed as (
    select
        string_field_0 as product_category_name,
        string_field_1 as product_category_name_english
    from source
)

select * from renamed