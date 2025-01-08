select
    style,
    count(*) as cnt,
    round(avg(recommendation_rank),2) as  avg_recommendation_rank,
    avg(would_recommend_to_friend_rank) as avg_recommend_to_friend_rank,
    avg(n_recs_purchased) average_recs_purchased,
    avg(n_recs_added_in_wantlist) average_recs_purchased
from feedback_fct ff
inner join release_meta_dim rmd 
    on ff.input_release_id = rmd.release_id
inner join release_styles_bridge rsb 
    on rmd.release_id = rsb.release_id
inner join styles_dim sd 
    on sd.style_id = rsb.style_id
group by style
order by count(*) desc