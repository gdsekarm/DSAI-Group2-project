

  create or replace view `stellar-verve-478012-n6`.`olist_raw`.`my_second_dbt_model`
  OPTIONS()
  as -- Use the `ref` function to select from other models

select *
from `stellar-verve-478012-n6`.`olist_raw`.`my_first_dbt_model`
where id = 1;

