# One off transformation for release metadata (static data)
import polars as pl
import polars.selectors as cs
from utils import S3Helper
import os
from io import BytesIO


def process_release_year(df):
    df = df.with_columns(
        pl.when(pl.col("release_year").cast(pl.String).str.len_chars() != 4)
        .then(None)
        .when(~pl.col("release_year").cast(pl.UInt32).is_between(1900, 2025))
        .then(None)
        .otherwise(pl.col("release_year").cast(pl.UInt32))
        .name.keep()
    )
    return df


def process_stats_cols(df):
    stats_cols = [
        col
        for col in df.select(cs.numeric()).columns
        if col not in ["release_id", "release_year"]
    ]
    df = df.with_columns(
        pl.when(pl.col(col) < 0).then(None).otherwise(pl.col(col)).alias(col)
        for col in stats_cols
    )
    return df


def process_empty_strings(df):
    df = df.with_columns(
        pl.when(pl.col(pl.String).str.len_chars() == 0)
        .then(None)
        .otherwise(pl.col(pl.String).str.strip_chars())
        .name.keep()
    )
    return df


def create_release_meta_dim(df):
    df = df.with_columns(pl.col("artist_name").list.join("/ ").alias("artist_name"))
    release_meta_columns = [
        col for col in df.columns if col not in ["genre", "is_master_release"]
    ]
    df = df.select(release_meta_columns)
    df = process_empty_strings(df)
    df = process_release_year(df)
    df = process_stats_cols(df)

    return df


def create_styles_dim(df):
    df = df["styles"].explode().unique().str.strip_chars().to_frame()
    df = df.with_columns(pl.int_range(0, df.shape[0]).alias("style_id")).rename(
        {"styles": "style"}
    )
    return df


def create_release_style_bridge(styles_df, release_meta_df):
    release_and_styles_df = (
        release_meta_df.select(["release_id", "styles"])
        .explode("styles")
        .with_columns(pl.col("styles").str.strip_chars())
    ).rename({"styles": "style"})
    release_style_bridge = release_and_styles_df.join(styles_df, on="style").select(
        ["release_id", "style_id"]
    )
    return release_style_bridge


def df_to_parquet_buffer(df):
    buffer = BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)
    return buffer


def main():
    s3_helper = S3Helper(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    release_meta_response = s3_helper.get_object(
        bucket="discogs-recommender",
        file_name="release_metadata/discogs_dataset.parquet",
        is_text=False,
    )
    df = pl.read_parquet(release_meta_response)
    release_meta_dim_df = create_release_meta_dim(df)
    styles_dim_df = create_styles_dim(df)
    release_style_bridge_df = create_release_style_bridge(
        styles_df=styles_dim_df, release_meta_df=release_meta_dim_df
    )
    dfs = {
        "release_meta_dim": release_meta_dim_df.drop("styles"),
        "styles_dim": styles_dim_df,
        "release_style_bridge": release_style_bridge_df,
    }

    for file_name, df in dfs.items():
        buffer = df_to_parquet_buffer(df)
        s3_helper.write_object(
            bucket="discogs-recommender",
            file_name=f"release_metadata/{file_name}.parquet",
            data=buffer,
            is_object=True,
        )


if __name__ == "__main__":
    main()
