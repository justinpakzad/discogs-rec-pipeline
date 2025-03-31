import sys
from pathlib import Path
import pytest
import polars as pl
import polars.selectors as cs

sys.path.append(str(Path(__file__).parents[1] / "src" / "scripts"))
from transform_and_load import (  # type: ignore
    create_release_meta_dim,
    create_styles_dim,
    create_release_style_bridge,
    process_release_year,
    process_stats_cols,
    process_empty_strings,
)


@pytest.fixture
def test_df():
    return pl.DataFrame(
        {
            "release_id": [29948860, 1822578, 14697753],
            "catno": ["ORBSLP005", "Laidrec 003", "UNLTD26"],
            "release_title": ["Seti", "First Darkness", "Unlimited Love #26"],
            "genre": [["Electronic"], ["Electronic"], ["Electronic"]],
            "styles": [["Ambient"], ["House", "Deep House"], ["House"]],
            "country": ["Worldwide", "Germany", "France"],
            "label_name": ["Orbscure", "Laid", "Unlimited Love"],
            "release_year": ["2024", "2009", "20201010"],
            "is_master_release": [False, None, None],
            "artist_name": [["Sedibus"], ["Rick Wade"], ["Various"]],
            "have": [-13.0, 498.0, 90.0],
            "want": [4.0, 270.0, 44.0],
            "avg_rating": [None, 4.49, 4.08],
            "ratings": [0.0, -95.0, 12.0],
            "low": [None, 3.19, 10.88],
            "median": [None, 6.02, 12.81],
            "high": [None, 9.99, 24.18],
            "want_to_have_ratio": [0.307692, 0.542169, 0.488889],
            "video_count": [5, 3, 4],
        }
    )


@pytest.fixture
def release_meta_schema():
    return {
        "release_id": "Int64",
        "catno": "String",
        "release_title": "String",
        "styles": "List(String)",
        "country": "String",
        "label_name": "String",
        "release_year": "UInt32",
        "artist_name": "String",
        "have": "Float64",
        "want": "Float64",
        "avg_rating": "Float64",
        "ratings": "Float64",
        "low": "Float64",
        "median": "Float64",
        "high": "Float64",
        "want_to_have_ratio": "Float64",
        "video_count": "Int64",
    }


@pytest.fixture
def styles_schema():
    return {"style": "String", "style_id": "Int64"}


@pytest.fixture
def release_style_schema():
    return {"release_id": "Int64", "style_id": "Int64"}


class TestReleaseMetaDim:
    def test_empty_strings(self, test_df):
        df = process_empty_strings(test_df)
        valid_cols = df.select(
            (pl.col(pl.String).str.strip_chars() != "").all()
        ).to_dict(as_series=False)
        assert all([val[0] for val in valid_cols.values()])

    def test_valid_release_year(self, test_df):
        df = process_release_year(process_empty_strings(test_df))
        assert (
            (df["release_year"].cast(pl.String).str.len_chars() == 4)
            & (df["release_year"].is_between(1900, 2025))
        ).all(ignore_nulls=True)

    def test_valid_stats(self, test_df):
        df = process_stats_cols(test_df)
        stats_cols = [
            col
            for col in df.select(cs.numeric()).columns
            if col not in ["release_id", "release_year"]
        ]
        valid_cols = df.select(
            (pl.col(stats_cols) >= 0).all(ignore_nulls=True)
        ).to_dict(as_series=False)
        assert all([val[0] for val in valid_cols.values()])

    def test_release_meta_dim_schema(self, test_df, release_meta_schema):
        release_meta_dim_df = create_release_meta_dim(test_df)
        release_meta_dim_schema = {
            k: str(v) for k, v in dict(release_meta_dim_df.schema).items()
        }
        assert release_meta_dim_schema == release_meta_schema


class TestStylesDim:
    def test_styles_dim_schema(self, test_df, styles_schema):
        styles_dim_df = create_styles_dim(test_df)
        styles_dim_schema = {k: str(v) for k, v in dict(styles_dim_df.schema).items()}
        assert styles_dim_schema == styles_schema


class TestReleaseStyleBridge:
    def test_release_style_bridge_schema(self, test_df, release_style_schema):
        release_meta_dim_df = create_release_meta_dim(test_df)
        styles_dim_df = create_styles_dim(test_df)
        release_style_bridge_df = create_release_style_bridge(
            styles_df=styles_dim_df, release_meta_df=release_meta_dim_df
        )
        release_style_dim_schema = {
            k: str(v) for k, v in dict(release_style_bridge_df.schema).items()
        }
        assert release_style_dim_schema == release_style_schema
