COPY INTO discogs_rec.public.release_meta_dim
FROM @justins_s3_stage/release_metadata/release_meta_dim.parquet
FILE_FORMAT = (FORMAT_NAME = 'my_parquet_format')
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';