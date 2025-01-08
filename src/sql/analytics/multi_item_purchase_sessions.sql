select
    device_type,
    device_os,
    avg(n_recs_purchased) as avg_items_purchased,
    count(*) as number_of_sessions
from feedback_fct
where purchased = 1 and n_recs_purchased > 1
group by device_type, device_os
order by avg_items_purchased desc;