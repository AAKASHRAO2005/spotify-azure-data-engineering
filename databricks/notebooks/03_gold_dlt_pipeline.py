# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Delta Live Tables (DLT) - Gold Layer Pipeline
# MAGIC Ingests streaming Silver tables, enforces data quality expectations, implements SCD Type 1 & Type 2 dimensional modeling, and creates Gold analytics marts.

# COMMAND ----------
import dlt
from pyspark.sql.functions import col, count, sum as _sum, avg, to_date, desc

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Silver Views with Data Expectations

# COMMAND ----------
@dlt.view(name="silver_user_source")
@dlt.expect("valid_user_id", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_subscription", "subscription_type IN ('Free', 'Premium', 'Family', 'Student')")
def silver_user_source():
    return spark.readStream.table("spotify_cata.silver.DimUser")


@dlt.view(name="silver_artist_source")
@dlt.expect("valid_artist_id", "artist_id IS NOT NULL")
def silver_artist_source():
    return spark.readStream.table("spotify_cata.silver.DimArtist")


@dlt.view(name="silver_track_source")
@dlt.expect("valid_track_id", "track_id IS NOT NULL")
@dlt.expect_or_drop("valid_duration", "duration_sec > 0")
def silver_track_source():
    return spark.readStream.table("spotify_cata.silver.DimTrack")


@dlt.table(name="gold_dim_date", comment="Curated Date Dimension")
def gold_dim_date():
    return spark.read.table("spotify_cata.silver.DimDate")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Slowly Changing Dimensions (SCD Type 2 & Type 1)

# COMMAND ----------
# DimUser: SCD Type 2 tracking subscription history
dlt.create_streaming_table(
    name="gold_dim_user_scd2",
    comment="SCD Type 2 User Dimension"
)

dlt.apply_changes(
    target="gold_dim_user_scd2",
    source="silver_user_source",
    keys=["user_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=2,
    track_history_column_list=["subscription_type", "country"]
)

# DimArtist: SCD Type 1 overwrite
dlt.create_streaming_table(
    name="gold_dim_artist_scd1",
    comment="SCD Type 1 Artist Dimension"
)

dlt.apply_changes(
    target="gold_dim_artist_scd1",
    source="silver_artist_source",
    keys=["artist_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=1
)

# DimTrack: SCD Type 1 overwrite
dlt.create_streaming_table(
    name="gold_dim_track_scd1",
    comment="SCD Type 1 Track Dimension"
)

dlt.apply_changes(
    target="gold_dim_track_scd1",
    source="silver_track_source",
    keys=["track_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=1
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Curated Gold Fact Table

# COMMAND ----------
@dlt.table(
    name="gold_fact_stream",
    comment="Curated Spotify Streaming Events",
    table_properties={"quality": "gold"}
)
@dlt.expect_or_drop("positive_listen_duration", "listen_duration > 0")
@dlt.expect("valid_user_fk", "user_id IS NOT NULL")
@dlt.expect("valid_track_fk", "track_id IS NOT NULL")
def gold_fact_stream():
    return (
        spark.readStream.table("spotify_cata.silver.FactStream")
        .withColumn("stream_date", to_date(col("stream_timestamp")))
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Business Reporting Marts

# COMMAND ----------
@dlt.table(name="gold_daily_streaming_metrics")
def gold_daily_streaming_metrics():
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


@dlt.table(name="gold_top_artists_by_streams")
def gold_top_artists_by_streams():
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
