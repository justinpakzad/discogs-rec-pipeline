CREATE OR REPLACE TABLE release_meta_dim (
    release_id BIGINT,
    catno VARCHAR,
    release_title VARCHAR,
    country VARCHAR,
    label_name VARCHAR,
    release_year INTEGER,
    artist_name VARCHAR,
    have DOUBLE PRECISION,
    want DOUBLE PRECISION,
    avg_rating DOUBLE PRECISION,
    ratings DOUBLE PRECISION,
    low DOUBLE PRECISION,
    median DOUBLE PRECISION,
    high DOUBLE PRECISION,
    want_to_have_ratio DOUBLE PRECISION,
    video_count BIGINT
);