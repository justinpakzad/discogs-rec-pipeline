select
    recommendation_relevance,
    count(*) as cnt,
    sum(n_recs_purchased) as total_recs_purchased,
    sum(n_recs_added_in_wantlist) as total_adds_to_wantlist,
    avg(session_duration) as avg_session_duration
from feedback_fct
group by recommendation_relevance