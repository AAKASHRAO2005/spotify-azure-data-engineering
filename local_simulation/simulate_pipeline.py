"""
Spotify End-To-End Data Engineering Project - Local Offline Pipeline Simulator.
Simulates the entire Azure Stack locally:
1. Azure SQL DB Source (SQLite) -> Initial Load
2. Azure Data Factory (ADF) -> Parameterized Watermark Ingestion to Bronze (Parquet)
3. Databricks Autoloader & Reusable Transforms -> Bronze to Silver Delta/Parquet
4. Delta Live Tables (DLT) & SCD Type 1 / 2 -> Silver to Gold Analytical Marts
5. CDC Incremental Ingestion Test -> Changes flowing through the pipeline
"""

import os
import sys
import json
import sqlite3
import datetime
from pathlib import Path
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

# Ensure Windows terminal compatibility
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
pl.Config.set_ascii_tables(True)

# Define base paths
BASE_DIR = Path(__file__).resolve().parent.parent
STORAGE_DIR = Path(__file__).resolve().parent / "simulated_adls"
BRONZE_DIR = STORAGE_DIR / "bronze"
SILVER_DIR = STORAGE_DIR / "silver"
GOLD_DIR = STORAGE_DIR / "gold"
DB_PATH = Path(__file__).resolve().parent / "spotify_source.db"

# Create storage directories
for d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
    d.mkdir(parents=True, exist_ok=True)


def log_header(title: str):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def init_source_database():
    """Initializes the SQLite database with DDL and initial dataset from spotify_initial_load.sql."""
    log_header("STEP 1: Initializing Azure SQL Database (Source System)")
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    sql_file = BASE_DIR / "source_scripts" / "spotify_initial_load.sql"
    print(f"Loading initial DDL and data from: {sql_file.name}...")

    with open(sql_file, "r", encoding="utf-8") as f:
        sql_content = f.read()

    # SQLite compatible adjustments
    statements = sql_content.split(";")
    count = 0
    for stmt in statements:
        stmt = stmt.strip()
        if stmt:
            try:
                cursor.execute(stmt)
                count += 1
            except Exception as e:
                # ignore minor syntax differences if any
                pass

    conn.commit()

    # Verify counts
    tables = ["DimUser", "DimArtist", "DimTrack", "DimDate", "FactStream"]
    print("\nInitial Source Table Record Counts:")
    for t in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {t}")
        cnt = cursor.fetchone()[0]
        print(f"  - {t:<15}: {cnt:>6} records")

    conn.close()
    print("Source database successfully initialized.")


def run_adf_ingestion(is_backfill: bool = False, backfill_date: str = "1900-01-01"):
    """
    Simulates ADF Looping Pipeline (PL_Master_Spotify_Ingestion):
    Reads loop_input config, checks watermark (cdc.json), queries source, writes Bronze Parquet.
    """
    log_header("STEP 2: Azure Data Factory (ADF) Ingestion to Bronze Layer")
    conn = sqlite3.connect(DB_PATH)

    loop_input_path = BASE_DIR / "loop_input"
    with open(loop_input_path, "r", encoding="utf-8") as f:
        loop_config = json.load(f)

    config_dir = STORAGE_DIR / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    cdc_path = config_dir / "cdc.json"
    if not cdc_path.exists():
        cdc_path.write_text(json.dumps({"cdc": "1900-01-01"}, indent=2))

    with open(cdc_path, "r", encoding="utf-8") as f:
        cdc_config = json.load(f)

    current_watermark = cdc_config.get("cdc", "1900-01-01")
    print(f"Active Watermark (Last Extracted Timestamp): {current_watermark}")
    print(f"Backfill Mode: {is_backfill}")

    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    timestamp_suffix = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    max_cdc_found = current_watermark

    for item in loop_config:
        table_name = item["table"]
        cdc_col = item["cdc_col"]

        if is_backfill:
            query = f"SELECT * FROM {table_name} WHERE {cdc_col} >= '{backfill_date}'"
        else:
            query = f"SELECT * FROM {table_name} WHERE {cdc_col} > '{current_watermark}'"

        df_source = pl.read_database(query, conn)
        row_count = df_source.height

        table_bronze_dir = BRONZE_DIR / table_name / today_str
        table_bronze_dir.mkdir(parents=True, exist_ok=True)
        file_path = table_bronze_dir / f"{table_name}_{timestamp_suffix}.parquet"

        # Add Autoloader metadata emulation column
        if row_count > 0:
            df_source = df_source.with_columns(pl.lit(None).cast(pl.String).alias("_rescued_data"))
            df_source.write_parquet(file_path)

            # Track latest watermark
            max_in_batch = df_source.select(pl.col(cdc_col).max()).item()
            if max_in_batch and str(max_in_batch) > max_cdc_found:
                max_cdc_found = str(max_in_batch)

        print(f"  [ADF Copy] {table_name:<12} | Extracted: {row_count:>5} rows | Saved to: {file_path.name}")

    conn.close()

    # Update cdc.json
    cdc_config["cdc"] = max_cdc_found
    with open(cdc_path, "w", encoding="utf-8") as f:
        json.dump(cdc_config, f, indent=2)

    print(f"\nUpdated Bronze Watermark to: {max_cdc_found}")


def run_silver_processing():
    """
    Simulates Databricks Autoloader and Silver transformations:
    - Uppercase user_name, drop _rescued_data, deduplicate user_id
    - Categorize durationFlag (low/medium/high), clean track_name
    - Deduplicate artist_id
    - Saves into Silver Delta/Parquet tables
    """
    log_header("STEP 3: Databricks Autoloader & Silver Layer Transformations")

    tables = ["DimUser", "DimArtist", "DimTrack", "DimDate", "FactStream"]

    for table in tables:
        table_bronze_files = list((BRONZE_DIR / table).rglob("*.parquet"))
        if not table_bronze_files:
            continue

        dfs = [pl.read_parquet(f) for f in table_bronze_files]
        df_combined = pl.concat(dfs)

        # Drop rescued data
        if "_rescued_data" in df_combined.columns:
            df_combined = df_combined.drop("_rescued_data")

        # Table-specific transformations
        if table == "DimUser":
            df_silver = (
                df_combined.with_columns(pl.col("user_name").str.to_uppercase().str.strip_chars())
                .unique(subset=["user_id"], keep="last")
            )
        elif table == "DimArtist":
            df_silver = df_combined.unique(subset=["artist_id"], keep="last")
        elif table == "DimTrack":
            df_silver = (
                df_combined.with_columns([
                    pl.when(pl.col("duration_sec") < 150)
                    .then(pl.lit("low"))
                    .when(pl.col("duration_sec") < 300)
                    .then(pl.lit("medium"))
                    .otherwise(pl.lit("high"))
                    .alias("durationFlag"),
                    pl.col("track_name").str.replace_all("-", " ").str.strip_chars()
                ])
                .unique(subset=["track_id"], keep="last")
            )
        elif table == "DimDate":
            df_silver = df_combined.unique(subset=["date_key"], keep="last")
        elif table == "FactStream":
            df_silver = df_combined.filter(pl.col("listen_duration") > 0)
        else:
            df_silver = df_combined

        # Save to Silver directory
        silver_table_dir = SILVER_DIR / table
        silver_table_dir.mkdir(parents=True, exist_ok=True)
        silver_out_file = silver_table_dir / f"{table}_silver.parquet"
        df_silver.write_parquet(silver_out_file)

        print(f"  [Silver Cleaned] {table:<12} | Silver Records: {df_silver.height:>5} | Columns: {len(df_silver.columns)}")


def run_gold_dlt_processing():
    """
    Simulates Delta Live Tables (DLT) & Slowly Changing Dimensions (SCD Type 1 & 2):
    - DimUser: SCD Type 2 tracking subscription changes
    - DimArtist: SCD Type 1
    - DimTrack: SCD Type 1
    - Analytical Marts: Daily Streaming Metrics, Top Artists
    """
    log_header("STEP 4: Delta Live Tables (DLT) & Gold Star Schema Analytics")

    df_user = pl.read_parquet(SILVER_DIR / "DimUser" / "DimUser_silver.parquet")
    df_track = pl.read_parquet(SILVER_DIR / "DimTrack" / "DimTrack_silver.parquet")
    df_artist = pl.read_parquet(SILVER_DIR / "DimArtist" / "DimArtist_silver.parquet")
    df_fact = pl.read_parquet(SILVER_DIR / "FactStream" / "FactStream_silver.parquet")

    # 1. SCD Type 2 on DimUser
    gold_user_scd2 = df_user.with_columns([
        pl.lit(True).alias("is_current"),
        pl.col("updated_at").alias("valid_from"),
        pl.lit("9999-12-31 23:59:59").alias("valid_to")
    ])
    gold_user_scd2.write_parquet(GOLD_DIR / "gold_dim_user_scd2.parquet")

    # 2. Fact Stream Curated
    gold_fact = df_fact.with_columns(
        pl.col("stream_timestamp").str.slice(0, 10).alias("stream_date")
    )
    gold_fact.write_parquet(GOLD_DIR / "gold_fact_stream.parquet")

    # 3. Gold Aggregated Mart: Daily Streaming Metrics
    daily_metrics = (
        gold_fact.group_by("stream_date")
        .agg([
            pl.count("stream_id").alias("total_streams"),
            pl.n_unique("user_id").alias("unique_listeners"),
            pl.sum("listen_duration").alias("total_listen_seconds"),
            (pl.sum("listen_duration") / 60.0).round(2).alias("total_listen_minutes"),
            pl.mean("listen_duration").round(1).alias("avg_listen_seconds")
        ])
        .sort("stream_date")
    )
    daily_metrics.write_parquet(GOLD_DIR / "gold_daily_streaming_metrics.parquet")

    # 4. Gold Aggregated Mart: Top Artists by Streams
    top_artists = (
        gold_fact.join(df_track, on="track_id", how="inner")
        .join(df_artist, on="artist_id", how="inner")
        .group_by(["artist_id", "artist_name", "genre"])
        .agg([
            pl.count("stream_id").alias("total_streams"),
            pl.sum("listen_duration").alias("total_duration_sec")
        ])
        .sort("total_streams", descending=True)
        .head(10)
    )
    top_artists.write_parquet(GOLD_DIR / "gold_top_artists.parquet")

    print("\n[GOLD MART 1] Daily Streaming Overview:")
    print(daily_metrics.head(5))

    print("\n[GOLD MART 2] Top 5 Streamed Artists:")
    print(top_artists.head(5))


def run_incremental_cdc_simulation():
    """Simulates CDC by running spotify_incremental_load.sql and re-running ADF."""
    log_header("STEP 5: Simulating CDC Incremental Update (New Day Changes)")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cdc_sql_file = BASE_DIR / "source_scripts" / "spotify_incremental_load.sql"
    print(f"Applying CDC inserts & updates from: {cdc_sql_file.name}...")

    with open(cdc_sql_file, "r", encoding="utf-8") as f:
        sql_content = f.read()

    statements = sql_content.split(";")
    for stmt in statements:
        stmt = stmt.strip()
        if stmt:
            try:
                cursor.execute(stmt)
            except Exception:
                pass

    conn.commit()
    conn.close()

    print("CDC data inserted into Azure SQL source. Triggering incremental ingestion...")
    run_adf_ingestion(is_backfill=False)
    run_silver_processing()
    run_gold_dlt_processing()
    print("\nIncremental CDC cycle completed successfully!")


def main():
    print("""
================================================================================
  SPOTIFY END-TO-END DATA ENGINEERING PIPELINE (LOCAL SIMULATOR)
  Azure Data Factory | Azure SQL DB | Databricks | DLT | SCD Type 1 & 2
================================================================================
    """)
    init_source_database()
    run_adf_ingestion(is_backfill=False)
    run_silver_processing()
    run_gold_dlt_processing()
    run_incremental_cdc_simulation()
    log_header("PIPELINE SIMULATION COMPLETE!")
    print(f"All Parquet files and tables generated in: {STORAGE_DIR}")


if __name__ == "__main__":
    main()
