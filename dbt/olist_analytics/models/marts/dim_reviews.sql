with reviews as (
    select * from {{ ref('stg_order_reviews') }}
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