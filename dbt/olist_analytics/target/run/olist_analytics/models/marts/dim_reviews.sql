
  
    

    create or replace table `stellar-verve-478012-n6`.`olist_raw_analytics`.`dim_reviews`
      
    
    

    OPTIONS()
    as (
      with reviews as (
    select * from `stellar-verve-478012-n6`.`olist_raw_staging`.`stg_order_reviews`
)

select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    created_at as review_created_at,
    answer_timestamp as review_answered_at
from reviews
    );
  