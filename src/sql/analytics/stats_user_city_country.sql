select 
    ud.country as user_country,
    ud.city as user_city,
    round(avg(recommendation_rank),2) as avg_recommendation_rank,
    round(avg(would_recommend_to_friend_rank) / 60,2) as avg_would_recommend_to_friend_rank,
    round(avg(n_clicks),2) as avg_links_clicked,
    round(avg(session_duration) / 60,2) as avg_session_duration_minutes,
    round(sum(n_recs_purchased),2) as n_recs_purchased,
    round(sum(n_recs_added_in_wantlist),2) as n_recs_added_in_wantlist,
    round(sum(shared_with_friends),2) as shared_with_friends_count
from feedback_fct ff
inner join user_dim ud on ff.user_id = ud.user_id
group by ud.country,ud.city