CREATE TABLE IF NOT EXISTS feedback_fct(
    feedback_id INTEGER,
    date_id INTEGER, 
    user_id VARCHAR,
    input_release_id INTEGER,
    feedback VARCHAR,
    recommendation_rank INTEGER,
    would_recommend_to_friend_rank INTEGER,
    purchased INTEGER, 
    added_to_wantlist INTEGER,
    familiarity VARCHAR,
    shared_with_friends INTEGER,
    recommendation_relevance VARCHAR,
    device_type VARCHAR,
    device_os VARCHAR,
    device_browser VARCHAR,
    session_duration INTEGER,
    n_clicks INTEGER,
    n_recs_purchased INTEGER,
    n_recs_added_in_wantlist INTEGER
);
