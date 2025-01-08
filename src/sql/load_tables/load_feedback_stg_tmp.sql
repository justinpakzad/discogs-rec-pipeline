COPY INTO discogs_rec.public.feedback_stg_tmp
FROM @justins_s3_stage/stg/{{ execution_date.strftime('%Y%m%d%H') }}/
FILE_FORMAT = (FORMAT_NAME = 'my_parquet_format')
PATTERN = '.*feedback_.*\.parquet'
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';