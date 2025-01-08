select
    count(*) as total_sessions,
    sum(case when purchased = 1 then 1 else 0 end) as purchased_sessions,
    round(sum(case when purchased = 1 then 1 else 0 end)::FLOAT / count(*) * 100,2) as purchase_rate,
    sum(case when added_to_wantlist = 1 then 1 else 0 end) as added_to_wantlist_sessions,
    round(sum(case when added_to_wantlist = 1 then 1 else 0 end)::FLOAT / count(*) * 100,2) as added_to_wantlist_rate
from feedback_fct;

