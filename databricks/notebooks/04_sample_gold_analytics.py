# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Spotify Gold Layer Analytics & Explorations
# MAGIC Visualizing and querying curated Gold tables and marts in Unity Catalog (`spotify_cata.gold`).

# COMMAND ----------
# MAGIC %sql
# MAGIC -- 1. Inspect Curated Dimensions and SCD Type 2 User Dimension
# MAGIC SELECT * FROM spotify_cata.gold.gold_dim_user_scd2 LIMIT 20;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- 2. Top Artists by Stream Counts
# MAGIC SELECT 
# MAGIC     artist_name, 
# MAGIC     genre, 
# MAGIC     total_streams, 
# MAGIC     total_listen_duration_sec,
# MAGIC     ROUND(total_listen_duration_sec / 60.0, 2) AS total_listen_minutes
# MAGIC FROM spotify_cata.gold.gold_top_artists_by_streams
# MAGIC ORDER BY total_streams DESC
# MAGIC LIMIT 10;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- 3. Daily Streaming KPIs
# MAGIC SELECT 
# MAGIC     stream_date,
# MAGIC     total_streams,
# MAGIC     stream_events,
# MAGIC     ROUND(total_listen_seconds / 60.0, 2) AS total_minutes,
# MAGIC     ROUND(avg_listen_duration_seconds, 1) AS avg_duration_sec
# MAGIC FROM spotify_cata.gold.gold_daily_streaming_metrics
# MAGIC ORDER BY stream_date DESC;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- 4. Subscription Type Distribution Among Active Listeners
# MAGIC SELECT 
# MAGIC     u.subscription_type,
# MAGIC     COUNT(f.stream_id) AS total_streams,
# MAGIC     COUNT(DISTINCT u.user_id) AS active_users
# MAGIC FROM spotify_cata.gold.gold_fact_stream f
# MAGIC JOIN spotify_cata.gold.gold_dim_user_scd2 u 
# MAGIC     ON f.user_id = u.user_id AND u.__IS_CURRENT = true
# MAGIC GROUP BY u.subscription_type
# MAGIC ORDER BY total_streams DESC;
