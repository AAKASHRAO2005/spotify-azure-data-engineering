"""
Unit tests for Jinja2 Star Schema Query Generation.
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "databricks", "src")))

from gold.jinja_star_schema import render_star_schema_query


def test_default_jinja_rendering():
    query = render_star_schema_query()
    assert "SELECT" in query
    assert "FROM" in query
    assert "LEFT JOIN" in query
    assert "spotify_cata.silver.FactStream" in query
    assert "spotify_cata.silver.DimUser" in query
    assert "spotify_cata.silver.DimTrack" in query
    assert "spotify_cata.silver.DimArtist" in query


def test_custom_parameters_rendering():
    custom_params = [
        {
            "table": "test_cata.silver.fact_test",
            "alias": "f",
            "cols": "f.id, f.metric"
        },
        {
            "table": "test_cata.silver.dim_item",
            "alias": "d",
            "cols": "d.item_name",
            "condition": "f.item_id = d.item_id"
        }
    ]
    query = render_star_schema_query(custom_params)
    assert "f.id, f.metric" in query
    assert "d.item_name" in query
    assert "test_cata.silver.fact_test AS f" in query
    assert "LEFT JOIN" in query
    assert "test_cata.silver.dim_item AS d" in query
    assert "f.item_id = d.item_id" in query


if __name__ == "__main__":
    pytest.main(["-v", __file__])
