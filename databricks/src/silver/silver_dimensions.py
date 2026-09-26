# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Spotify Medallion Architecture - Silver Layer Processing
# MAGIC Ingestion from ADLS Gen2 Bronze Container into Unity Catalog Silver Layer using **Databricks Autoloader (Structured Streaming)**.

# COMMAND ----------
import os
import sys
from pyspark.sql.functions import col, upper, when, regexp_replace, current_timestamp
from pyspark.sql.types import *

# Add root directory to sys.path for modular imports
project_pth = os.path.abspath(os.path.join(os.getcwd(), '..', '..'))
if project_pth not in sys.path:
    sys.path.append(project_pth)

try:
    from utils.transformations import reusable
except ImportError:
    from databricks.src.utils.transformations import reusable

# Configuration parameters / widgets
storage_account = dbutils.widgets.get("storage_account") if "dbutils" in globals() else "storageazureproject"
catalog_name = dbutils.widgets.get("catalog_name") if "dbutils" in globals() else "spotify_cata"
bronze_base = f"abfss://bronze@{storage_account}.dfs.core.windows.net"
silver_base = f"abfss://silver@{storage_account}.dfs.core.windows.net"

transformer = reusable()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. DimUser Ingestion (Bronze -> Silver)

# COMMAND ----------
df_user_stream = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"{silver_base}/DimUser/checkpoint") \
    .option("schemaEvolutionMode", "addNewColumns") \
    .load(f"{bronze_base}/DimUser")

# Transformations: Upper case user_name, drop rescued data, deduplicate on user_id
df_user = df_user_stream.withColumn("user_name", upper(col("user_name")))
df_user = transformer.dropColumns(df_user, ["_rescued_data"])
df_user = transformer.deduplicate(df_user, ["user_id"])

user_query = df_user.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{silver_base}/DimUser/checkpoint") \
    .trigger(availableNow=True) \
    .option("path", f"{silver_base}/DimUser/data") \
    .toTable(f"{catalog_name}.silver.DimUser")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. DimArtist Ingestion (Bronze -> Silver)

# COMMAND ----------
df_art_stream = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"{silver_base}/DimArtist/checkpoint") \
    .option("schemaEvolutionMode", "addNewColumns") \
    .load(f"{bronze_base}/DimArtist")

df_art = transformer.dropColumns(df_art_stream, ["_rescued_data"])
df_art = transformer.deduplicate(df_art, ["artist_id"])

art_query = df_art.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{silver_base}/DimArtist/checkpoint") \
    .trigger(availableNow=True) \
    .option("path", f"{silver_base}/DimArtist/data") \
    .toTable(f"{catalog_name}.silver.DimArtist")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. DimTrack Ingestion (Bronze -> Silver)

# COMMAND ----------
df_track_stream = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"{silver_base}/DimTrack/checkpoint") \
    .option("schemaEvolutionMode", "addNewColumns") \
    .load(f"{bronze_base}/DimTrack")

# Add duration flag and clean track names
df_track = transformer.add_duration_flag(df_track_stream, duration_col="duration_sec", flag_col="durationFlag")
df_track = transformer.clean_track_names(df_track, column_name="track_name")
df_track = transformer.dropColumns(df_track, ["_rescued_data"])

track_query = df_track.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{silver_base}/DimTrack/checkpoint") \
    .trigger(availableNow=True) \
    .option("path", f"{silver_base}/DimTrack/data") \
    .toTable(f"{catalog_name}.silver.DimTrack")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. DimDate Ingestion (Bronze -> Silver)

# COMMAND ----------
df_date_stream = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"{silver_base}/DimDate/checkpoint") \
    .option("schemaEvolutionMode", "addNewColumns") \
    .load(f"{bronze_base}/DimDate")

df_date = transformer.dropColumns(df_date_stream, ["_rescued_data"])

date_query = df_date.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{silver_base}/DimDate/checkpoint") \
    .trigger(availableNow=True) \
    .option("path", f"{silver_base}/DimDate/data") \
    .toTable(f"{catalog_name}.silver.DimDate")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. FactStream Ingestion (Bronze -> Silver)

# COMMAND ----------
df_fact_stream = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"{silver_base}/FactStream/checkpoint") \
    .option("schemaEvolutionMode", "addNewColumns") \
    .load(f"{bronze_base}/FactStream")

df_fact = transformer.dropColumns(df_fact_stream, ["_rescued_data"])

fact_query = df_fact.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{silver_base}/FactStream/checkpoint") \
    .trigger(availableNow=True) \
    .option("path", f"{silver_base}/FactStream/data") \
    .toTable(f"{catalog_name}.silver.FactStream")

print("All silver streaming ingestion pipelines triggered successfully.")
