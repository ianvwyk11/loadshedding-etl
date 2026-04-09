"""
test_transform.py
-----------------
Unit tests for the transform module.
"""

import pytest
import pandas as pd
from etl.transform import transform_status


MOCK_STATUS = {
    "status": {
        "eskom": {
            "stage": "4",
            "stage_updated": "2024-01-15T08:00:00+02:00",
            "next_stages": [{"stage": 2, "stage_start_timestamp": "2024-01-15T22:00:00+02:00"}]
        },
        "capetown": {
            "stage": "2",
            "stage_updated": "2024-01-15T06:00:00+02:00",
            "next_stages": []
        }
    }
}


def test_transform_status_returns_dataframe():
    df = transform_status(MOCK_STATUS)
    assert isinstance(df, pd.DataFrame)


def test_transform_status_row_count():
    df = transform_status(MOCK_STATUS)
    assert len(df) == 2


def test_transform_status_columns():
    df = transform_status(MOCK_STATUS)
    expected_cols = {"region", "stage", "stage_updated", "next_stages", "extracted_at"}
    assert expected_cols.issubset(set(df.columns))


def test_transform_status_stage_is_int():
    df = transform_status(MOCK_STATUS)
    assert df["stage"].dtype == int


def test_transform_status_stage_values():
    df = transform_status(MOCK_STATUS)
    stages = set(df["stage"].tolist())
    assert stages == {4, 2}


def test_transform_status_empty_input():
    df = transform_status({"status": {}})
    assert len(df) == 0
