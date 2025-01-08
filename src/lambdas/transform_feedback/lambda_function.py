import polars as pl
import boto3
from io import StringIO
import json
import logging
import s3fs
import datetime as dt


def parse_user_location(df):
    df = df.with_columns(
        [
            pl.col("location")
            .struct.field("country")
            .str.strip_chars()
            .str.to_titlecase()
            .alias("country"),
            pl.col("location")
            .struct.field("city")
            .str.strip_chars()
            .str.to_titlecase()
            .alias("city"),
        ]
    )
    return df


def parse_user_device(df):
    df = df.with_columns(
        [
            pl.col("device")
            .struct.field("type")
            .str.strip_chars()
            .str.to_titlecase()
            .alias("device_type"),
            pl.col("device").struct.field("os").str.strip_chars().alias("device_os"),
            pl.col("device")
            .struct.field("browser")
            .str.strip_chars()
            .str.to_titlecase()
            .alias("device_browser"),
        ]
    )
    return df


def parse_user_session(df):
    df = df.with_columns(
        [
            pl.col("session")
            .struct.field("session_id")
            .str.strip_chars()
            .alias("session_id"),
            pl.col("session")
            .struct.field("session_duration")
            .cast(pl.UInt32)
            .alias("session_duration"),
            pl.col("session")
            .struct.field("n_clicks")
            .cast(pl.UInt32)
            .alias("n_clicks"),
        ]
    )
    return df


def parse_user_activity(df):
    df = df.with_columns(
        [
            pl.col("purchased")
            .struct.field("status")
            .str.strip_chars()
            .alias("purchased"),
            pl.col("purchased")
            .struct.field("count")
            .cast(pl.UInt32)
            .alias("n_recs_purchased"),
            pl.col("added_to_wantlist")
            .struct.field("status")
            .str.strip_chars()
            .alias("added_to_wantlist"),
            pl.col("added_to_wantlist")
            .struct.field("count")
            .cast(pl.UInt32)
            .alias("n_recs_added_in_wantlist"),
        ]
    )
    return df


def clean_string_cols(df):
    df = df.with_columns(
        pl.when(
            (pl.col(pl.String).str.strip_chars() == "")
            | (pl.col(pl.String).str.strip_chars().str.to_lowercase() == "n/a")
            | (pl.col(pl.String).str.strip_chars().str.to_lowercase() == "none")
        )
        .then(None)
        .otherwise(pl.col(pl.String).str.strip_chars())
        .name.keep()
    )
    return df


def convert_string_to_numeric(df):
    columns = [col for col in df.columns if col.endswith("rank") or col == "age"]
    df = df.with_columns(pl.col(columns).cast(pl.UInt32).name.keep())
    return df


def create_boolean_flags(df):
    columns = [
        "shared_with_friends",
        "added_to_wantlist",
        "purchased",
    ]
    df = df.with_columns(
        pl.when(pl.col(columns).str.strip_chars().str.to_lowercase() == "no")
        .then(0)
        .when(pl.col(columns).str.strip_chars().str.to_lowercase() == "yes")
        .then(1)
        .otherwise(None)
        .name.keep()
    )
    return df


def drop_dups_and_format_dates(df):
    df = df.with_columns(pl.col("interaction_timestamp").str.to_datetime(strict=False))
    df = df.unique(subset=["session_id", "user_id", "interaction_timestamp", "email"])
    return df


def transform_df(df):
    return (
        df.pipe(parse_user_location)
        .pipe(parse_user_device)
        .pipe(parse_user_session)
        .pipe(parse_user_activity)
        .drop(["location", "device", "session"])
        .pipe(clean_string_cols)
        .pipe(convert_string_to_numeric)
        .pipe(create_boolean_flags)
        .pipe(drop_dups_and_format_dates)
    )


def get_s3_data(s3, bucket, file_name):
    try:
        response = s3.get_object(Bucket=bucket, Key=file_name)
        file_content = response["Body"].read().decode("utf-8")
        return file_content
    except Exception as e:
        logging.error(f"Error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error processing data: {str(e)}"),
        }


def list_objects(s3, bucket, folder=None):
    try:
        response = (
            s3.list_objects_v2(Bucket=bucket, Prefix=folder)
            if folder
            else s3.list_objects_v2(Bucket=bucket)
        )
        return response
    except Exception as e:
        logging.error(f"Error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error fetching data: {str(e)}"),
        }


def write_parquet_to_s3(bucket, file_name, df):
    fs = s3fs.S3FileSystem()
    try:
        with fs.open(f"s3://{bucket}/{file_name}", mode="wb") as f:
            df.write_parquet(f)
            return True
    except Exception as e:
        logging.error(f"Error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error writing parquet : {str(e)}"),
        }


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    bucket = "discogs-recommender"
    todays_date = dt.datetime.now().strftime("%Y%m%d%H")
    objects = list_objects(s3, bucket=bucket, folder=f"logs/{todays_date}")
    todays_files = [
        obj.get("Key")
        for obj in objects.get("Contents", [])
        if obj.get("Key").endswith("json")
    ]
    for obj in todays_files:
        s3_data = get_s3_data(s3=s3, bucket=bucket, file_name=obj)
        df = pl.read_json(StringIO(s3_data), infer_schema_length=2500)
        transformed_df = transform_df(df)
        write_parquet_to_s3(
            bucket=bucket,
            file_name=(
                f"stg/{todays_date}/feedback_"
                f"{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            ),
            df=transformed_df,
        )
