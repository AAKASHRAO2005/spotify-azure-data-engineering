# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Spotify Medallion Architecture - Silver Layer Ingestion
# MAGIC Reads raw Parquet files from ADLS Gen2 Bronze containers using **Databricks Autoloader (Structured Streaming)**, applies cleansing & deduplication, and writes to Unity Catalog Silver tables.

# COMMAND ----------
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------
import os
import sys
from pyspark.sql.functions import col, upper, when, regexp_replace
from pyspark.sql.types import *

# Define reusable transformation class directly or import
class reusable:
    def dropColumns(self, df, cols):
        existing_cols = set(df.columns)
        return df.drop(*[c for c in cols if c in existing_cols])

    def clean_track_names(self, df, column_name="track_name"):
        return df.withColumn(column_name, regexp_replace(col(column_name), "-", " "))

    def add_duration_flag(self, df, duration_col="duration_sec", flag_col="durationFlag"):
        return df.withColumn(
            flag_col,
            when(col(duration_col) < 150, "low")
            .when(col(duration_col) < 300, "medium")
            .otherwise("high")
        )

transformer = reusable()

# Storage configuration
storage_account = dbutils.widgets.get("storage_account") if "dbutils" in globals() else "storageazureproject"
catalog_name = dbutils.widgets.get("catalog_name") if "dbutils" in globals() else "spotify_cata"

bronze_base = f"abfss://bronze@{storage_account}.dfs.core.windows.net"
silver_base = f"abfss://silver@{storage_account}.dfs.core.windows.net"

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Ingest DimUser

# COMMAND ----------
df_user = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"{silver_base}/DimUser/checkpoint") \
    .option("schemaEvolutionMode", "addNewColumns") \
    .load(f"{bronze_base}/DimUser")

df_user = df_user.withColumn("user_name", upper(col("user_name")))
df_user = transformer.dropColumns(df_user, ["_rescued_data"])
df_user = df_user.dropDuplicates(["user_id"])

df_user.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{silver_base}/DimUser/checkpoint") \
    .trigger(availableNow=True) \
    .option("path", f"{silver_base}/DimUser/data") \
    .toTable(f"{catalog_name}.silver.DimUser")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Ingest DimArtist

# COMMAND ----------
df_art = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"{silver_base}/DimArtist/checkpoint") \
    .option("schemaEvolutionMode", "addNewColumns") \
    .load(f"{bronze_base}/DimArtist")

df_art = transformer.dropColumns(df_art, ["_rescued_data"])
df_art = df_art.dropDuplicates(["artist_id"])

df_art.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{silver_base}/DimArtist/checkpoint") \
    .trigger(availableNow=True) \
    .option("path", f"{silver_base}/DimArtist/data") \
    .toTable(f"{catalog_name}.silver.DimArtist")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Ingest DimTrack

# COMMAND ----------
df_track = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"{silver_base}/DimTrack/checkpoint") \
    .option("schemaEvolutionMode", "addNewColumns") \
    .load(f"{bronze_base}/DimTrack")

df_track = transformer.add_duration_flag(df_track, "duration_sec", "durationFlag")
df_track = transformer.clean_track_names(df_track, "track_name")
df_track = transformer.dropColumns(df_track, ["_rescued_data"])

df_track.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{silver_base}/DimTrack/checkpoint") \
    .trigger(availableNow=True) \
    .option("path", f"{silver_base}/DimTrack/data") \
    .toTable(f"{catalog_name}.silver.DimTrack")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Ingest DimDate

# COMMAND ----------
df_date = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"{silver_base}/DimDate/checkpoint") \
    .option("schemaEvolutionMode", "addNewColumns") \
    .load(f"{bronze_base}/DimDate")

df_date = transformer.dropColumns(df_date, ["_rescued_data"])

df_date.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{silver_base}/DimDate/checkpoint") \
    .trigger(availableNow=True) \
    .option("path", f"{silver_base}/DimDate/data") \
    .toTable(f"{catalog_name}.silver.DimDate")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Ingest FactStream

# COMMAND ----------
df_fact = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"{silver_base}/FactStream/checkpoint") \
    .option("schemaEvolutionMode", "addNewColumns") \
    .load(f"{bronze_base}/FactStream")

df_fact = transformer.dropColumns(df_fact, ["_rescued_data"])

df_fact.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{silver_base}/FactStream/checkpoint") \
    .trigger(availableNow=True) \
    .option("path", f"{silver_base}/FactStream/data") \
    .toTable(f"{catalog_name}.silver.FactStream")
