INSERT INTO feedback_fct (
    feedback_id,
    date_id,
    user_id,
    input_release_id,
    feedback,
    recommendation_rank,
    would_recommend_to_friend_rank,
    shared_with_friends,
    familiarity,
    purchased,
    n_recs_purchased,
    added_to_wantlist,
    n_recs_added_in_wantlist,
    recommendation_relevance,
    device_type,
    device_os,
    device_browser,
    n_clicks,
    session_duration
)
WITH user_feedback_cleaned as (
SELECT 
    CAST(row_number() OVER (ORDER BY 1) + (SELECT COALESCE(MAX(feedback_id), 0) FROM feedback_fct) AS INTEGER) AS feedback_id,
    CAST(REPLACE(interaction_timestamp::date, '-', '') AS INTEGER) AS date_id,
    CAST(user_id AS VARCHAR) AS user_id,
    CAST(ARRAY_REVERSE(SPLIT(input_url, '/'))[0] AS INTEGER) AS input_release_id,
    CAST(feedback AS VARCHAR) AS feedback,
    CAST(recommendation_rank AS INTEGER) AS recommendation_rank,
    CAST(would_recommend_to_friend_rank AS INTEGER) AS would_recommend_to_friend_rank,
    CAST(shared_with_friends AS INTEGER) AS shared_with_friends,
    CAST(familiarity AS VARCHAR) AS familiarity,
    CAST(purchased AS INTEGER) AS purchased,
    CAST(n_recs_purchased AS INTEGER) AS n_recs_purchased,
    CAST(added_to_wantlist AS INTEGER) AS added_to_wantlist,
    CAST(n_recs_added_in_wantlist AS INTEGER) AS n_recs_added_in_wantlist,
    CAST(recommendation_relevance AS VARCHAR) AS recommendation_relevance,
    CAST(device_type AS VARCHAR) AS device_type,
    CAST(device_os AS VARCHAR) AS device_os,
    CAST(device_browser AS VARCHAR) AS device_browser,
    CAST(n_clicks AS INTEGER) AS n_clicks,
    CAST(session_duration AS INTEGER) AS session_duration,
    ROW_NUMBER() OVER(PARTITION BY user_id,interaction_timestamp order by interaction_timestamp desc) as rn
FROM feedback_stg_tmp)
SELECT 
    feedback_id,
    date_id,
    user_id,
    input_release_id,
    feedback,
    recommendation_rank,
    would_recommend_to_friend_rank,
    shared_with_friends,
    familiarity,
    purchased,
    n_recs_purchased,
    added_to_wantlist,
    n_recs_added_in_wantlist,
    recommendation_relevance,
    device_type,
    device_os,
    device_browser,
    n_clicks,
    session_duration
FROM user_feedback_cleaned
WHERE rn = 1
    