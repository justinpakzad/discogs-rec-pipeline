MERGE INTO user_dim AS target
USING (
    WITH users_interaction_dates AS (
        SELECT
            user_id,
            MIN(date) AS first_interaction_date,
            MAX(date) AS latest_interaction_date
        FROM feedback_fct ff
        INNER JOIN date_dim dd ON ff.date_id = dd.date_id
        GROUP BY user_id
    ),
    user_dim_cleaned AS (
        SELECT
            fs.user_id::VARCHAR AS user_id,
            email::VARCHAR AS email,
            username::VARCHAR AS username,
            age::INTEGER AS age,
            country::VARCHAR AS country,
            city::VARCHAR AS city,
            COALESCE(uid.first_interaction_date, interaction_timestamp::DATE) AS first_interaction_date,
            COALESCE(uid.latest_interaction_date, interaction_timestamp::DATE) AS latest_interaction_date
        FROM feedback_stg_tmp fs
        LEFT JOIN users_interaction_dates uid
        ON fs.user_id = uid.user_id
    )
    SELECT * FROM user_dim_cleaned
) AS source
ON source.user_id = target.user_id
WHEN MATCHED THEN
    UPDATE SET
        target.email = source.email,
        target.username = source.username,
        target.age = source.age,
        target.country = source.country,
        target.city = source.city,
        target.first_interaction_date = source.first_interaction_date,
        target.latest_interaction_date = source.latest_interaction_date
WHEN NOT MATCHED THEN
    INSERT (
        user_id,
        email,
        username,
        age,
        country,
        city,
        first_interaction_date,
        latest_interaction_date
    )
    VALUES (
        source.user_id,
        source.email,
        source.username,
        source.age,
        source.country,
        source.city,
        source.first_interaction_date,
        source.latest_interaction_date
    );
