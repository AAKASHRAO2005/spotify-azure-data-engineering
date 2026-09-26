"""
Delta Live Tables (DLT) Pipeline for Spotify Gold Layer.
Implements:
1. Data Quality Expectations (validity, completeness)
2. Slowly Changing Dimensions (SCD Type 2 for Users, SCD Type 1 for Artists and Tracks)
3. Curated Star Schema Fact Tables
4. Business Analytics Aggregation Marts
"""
import dlt
from pyspark.sql.functions import col, count, sum as _sum, avg, to_date, desc, current_timestamp

# ==============================================================================
# 1. STREAMING INGESTION FROM SILVER LAYER WITH DATA EXPECTATIONS
# ==============================================================================

@dlt.view(name="silver_user_source")
@dlt.expect("valid_user_id", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_subscription", "subscription_type IN ('Free', 'Premium', 'Family', 'Student')")
def silver_user_source():
    """Reads streaming updates from Silver DimUser."""
    return spark.readStream.table("spotify_cata.silver.DimUser")


@dlt.view(name="silver_artist_source")
@dlt.expect("valid_artist_id", "artist_id IS NOT NULL")
def silver_artist_source():
    """Reads streaming updates from Silver DimArtist."""
    return spark.readStream.table("spotify_cata.silver.DimArtist")


@dlt.view(name="silver_track_source")
@dlt.expect("valid_track_id", "track_id IS NOT NULL")
@dlt.expect_or_drop("valid_duration", "duration_sec > 0")
def silver_track_source():
    """Reads streaming updates from Silver DimTrack."""
    return spark.readStream.table("spotify_cata.silver.DimTrack")


@dlt.table(
    name="gold_dim_date",
    comment="Curated Date Dimension for Analytical Reporting"
)
def gold_dim_date():
    """Loads Date dimension into Gold layer."""
    return spark.read.table("spotify_cata.silver.DimDate")


# ==============================================================================
# 2. SLOWLY CHANGING DIMENSIONS (SCD TYPE 1 & TYPE 2) VIA DLT APPLY_CHANGES
# ==============================================================================

# SCD Type 2 for DimUser: Tracks user subscription changes over time with __START_AT and __END_AT
dlt.create_streaming_table(
    name="gold_dim_user_scd2",
    comment="Slowly Changing Dimension Type 2 for Spotify Users"
)

dlt.apply_changes(
    target="gold_dim_user_scd2",
    source="silver_user_source",
    keys=["user_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=2,
    track_history_column_list=["subscription_type", "country"]
)


# SCD Type 1 for DimArtist: Overwrites changes to maintain latest state
dlt.create_streaming_table(
    name="gold_dim_artist_scd1",
    comment="Slowly Changing Dimension Type 1 for Spotify Artists"
)

dlt.apply_changes(
    target="gold_dim_artist_scd1",
    source="silver_artist_source",
    keys=["artist_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=1
)


# SCD Type 1 for DimTrack: Overwrites updates with latest metadata
dlt.create_streaming_table(
    name="gold_dim_track_scd1",
    comment="Slowly Changing Dimension Type 1 for Spotify Tracks"
)

dlt.apply_changes(
    target="gold_dim_track_scd1",
    source="silver_track_source",
    keys=["track_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=1
)


# ==============================================================================
# 3. CURATED FACT STREAM TABLE
# ==============================================================================

@dlt.table(
    name="gold_fact_stream",
    comment="Cleaned, validated Spotify streaming events",
    table_properties={"quality": "gold"}
)
@dlt.expect_or_drop("positive_listen_duration", "listen_duration > 0")
@dlt.expect("valid_user_fk", "user_id IS NOT NULL")
@dlt.expect("valid_track_fk", "track_id IS NOT NULL")
def gold_fact_stream():
    """Reads streaming events and enriches with date/time attributes."""
    return (
        spark.readStream.table("spotify_cata.silver.FactStream")
        .withColumn("stream_date", to_date(col("stream_timestamp")))
    )


# ==============================================================================
# 4. BUSINESS ANALYTICS GOLD MARTS
# ==============================================================================

@dlt.table(
    name="gold_daily_streaming_metrics",
    comment="Aggregated daily metrics: total streams, active users, total minutes streamed"
)
def gold_daily_streaming_metrics():
    """Computes daily high-level streaming KPIs."""
    fact = dlt.read("gold_fact_stream")
    return (
        fact.groupBy("stream_date")
        .agg(
            count("stream_id").alias("total_streams"),
            count("user_id").alias("stream_events"),
            _sum("listen_duration").alias("total_listen_seconds"),
            avg("listen_duration").alias("avg_listen_duration_seconds")
        )
    )


@dlt.table(
    name="gold_top_artists_by_streams",
    comment="Top ranked artists by total stream counts and unique listeners"
)
def gold_top_artists_by_streams():
    """Joins fact streams with artists and tracks to evaluate top performers."""
    fact = dlt.read("gold_fact_stream")
    track = dlt.read("gold_dim_track_scd1")
    artist = dlt.read("gold_dim_artist_scd1")

    return (
        fact.join(track, on="track_id", how="inner")
        .join(artist, on="artist_id", how="inner")
        .groupBy("artist.artist_id", "artist.artist_name", "artist.genre")
        .agg(
            count("fact.stream_id").alias("total_streams"),
            _sum("fact.listen_duration").alias("total_listen_duration_sec")
        )
        .orderBy(desc("total_streams"))
    )
