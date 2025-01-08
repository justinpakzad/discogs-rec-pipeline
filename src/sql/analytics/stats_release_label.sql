select 
    label_name,
    avg(recommendation_rank) as avg_recommendation_rank,
    avg(would_recommend_to_friend_rank) as avg_would_recommend_to_friend_rank,
    avg(session_duration) as avg_session_duration,
    count(*) as cnt
from feedback_fct ff
inner join release_meta_dim rmd on ff.input_release_id = rmd.release_id
group by label_name
order by count(*) desc
