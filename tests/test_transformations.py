"""
Unit tests for reusable transformation utilities.
Tests run both locally (with mock DataFrames) and in Spark CI environments.
"""
import pytest
import sys
import os
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "databricks", "src")))

try:
    from utils.transformations import reusable
except ImportError:
    reusable = None


def test_reusable_class_instantiation():
    assert reusable is not None
    transformer = reusable()
    assert hasattr(transformer, "dropColumns")
    assert hasattr(transformer, "standardize_user_names")
    assert hasattr(transformer, "clean_track_names")
    assert hasattr(transformer, "add_duration_flag")
    assert hasattr(transformer, "deduplicate")


def test_drop_columns_logic():
    transformer = reusable()
    mock_df = MagicMock()
    mock_df.columns = ["user_id", "user_name", "_rescued_data"]
    mock_df.drop.return_value = "dropped_df"

    result = transformer.dropColumns(mock_df, ["_rescued_data", "non_existent_col"])
    mock_df.drop.assert_called_once_with("_rescued_data")
    assert result == "dropped_df"


def test_deduplicate_logic():
    transformer = reusable()
    mock_df = MagicMock()
    mock_df.dropDuplicates.return_value = "deduped_df"

    result = transformer.deduplicate(mock_df, ["user_id"])
    mock_df.dropDuplicates.assert_called_once_with(["user_id"])
    assert result == "deduped_df"


if __name__ == "__main__":
    pytest.main(["-v", __file__])
