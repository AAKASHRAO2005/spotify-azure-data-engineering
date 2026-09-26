"""
reusable utilities for PySpark transformations in the Spotify Data Engineering pipeline.
"""
try:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import col, upper, trim, when, regexp_replace
except ImportError:
    DataFrame = object
    col = upper = trim = when = regexp_replace = None
from typing import List


class reusable:
    """
    Reusable transformation helper methods for PySpark DataFrames in Silver & Gold layers.
    """

    def __init__(self):
        pass

    def dropColumns(self, df: DataFrame, cols: List[str]) -> DataFrame:
        """
        Drops specified columns from DataFrame if they exist.
        Commonly used to drop `_rescued_data` column produced by Databricks Autoloader.
        """
        existing_cols = set(df.columns)
        cols_to_drop = [c for c in cols if c in existing_cols]
        return df.drop(*cols_to_drop)

    def standardize_user_names(self, df: DataFrame, column_name: str = "user_name") -> DataFrame:
        """
        Standardizes user names by converting to uppercase and trimming whitespace.
        """
        if column_name in df.columns:
            return df.withColumn(column_name, upper(trim(col(column_name))))
        return df

    def clean_track_names(self, df: DataFrame, column_name: str = "track_name") -> DataFrame:
        """
        Replaces hyphens with spaces and trims track names.
        """
        if column_name in df.columns:
            return df.withColumn(column_name, trim(regexp_replace(col(column_name), "-", " ")))
        return df

    def add_duration_flag(
        self,
        df: DataFrame,
        duration_col: str = "duration_sec",
        flag_col: str = "durationFlag",
        low_threshold: int = 150,
        medium_threshold: int = 300,
    ) -> DataFrame:
        """
        Categorizes song durations:
        - < 150 sec -> 'low'
        - 150 - 299 sec -> 'medium'
        - >= 300 sec -> 'high'
        """
        if duration_col in df.columns:
            return df.withColumn(
                flag_col,
                when(col(duration_col) < low_threshold, "low")
                .when(col(duration_col) < medium_threshold, "medium")
                .otherwise("high"),
            )
        return df

    def deduplicate(self, df: DataFrame, key_cols: List[str]) -> DataFrame:
        """
        Deduplicates DataFrame records based on primary key columns.
        """
        return df.dropDuplicates(key_cols)
