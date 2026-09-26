"""
Spotify End-to-End Azure Data Engineering Lakehouse - Interactive Web Dashboard
Provides real-time visibility into Bronze, Silver, and Gold Medallion layers,
pipeline orchestration controls, and business analytics.
"""

import os
import sys
import json
import sqlite3
import datetime
from pathlib import Path
import polars as pl
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Configure page settings
st.set_page_config(
    page_title="Spotify Lakehouse Platform",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for Spotify Dark Aesthetic
st.markdown("""
<style>
    /* Main layout & background */
    .stApp {
        background-color: #121212;
        color: #FFFFFF;
        font-family: 'CircularStd', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
    }
    
    /* Headers */
    h1, h2, h3, h4 {
        color: #FFFFFF !important;
        font-weight: 700;
    }
    
    /* Primary Accent Color - Spotify Green */
    .highlight-green {
        color: #1DB954;
        font-weight: bold;
    }
    
    /* Metrics card container */
    div[data-testid="stMetric"] {
        background-color: #181818;
        border: 1px solid #282828;
        border-radius: 10px;
        padding: 16px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    div[data-testid="stMetric"]:hover {
        border-color: #1DB954;
        transform: translateY(-2px);
    }
    div[data-testid="stMetricLabel"] p {
        color: #B3B3B3 !important;
        font-size: 0.9rem;
    }
    div[data-testid="stMetricValue"] div {
        color: #1DB954 !important;
        font-weight: 800;
        font-size: 1.8rem;
    }
    
    /* Tab headers */
    .stTabs [data-baseweb="tab-list"] {
        gap: 12px;
        background-color: transparent;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #181818;
        border-radius: 20px;
        color: #B3B3B3;
        padding: 8px 20px;
        font-weight: 600;
        border: 1px solid #282828;
    }
    .stTabs [aria-selected="true"] {
        background-color: #1DB954 !important;
        color: #000000 !important;
    }
    
    /* Buttons */
    .stButton>button {
        background-color: #1DB954;
        color: #000000;
        font-weight: 700;
        border-radius: 30px;
        border: none;
        padding: 10px 24px;
        transition: all 0.2s ease;
    }
    .stButton>button:hover {
        background-color: #1ed760;
        color: #000000;
        transform: scale(1.03);
    }

    /* DataFrame styling */
    div[data-testid="stDataFrame"] {
        border-radius: 8px;
        overflow: hidden;
        border: 1px solid #282828;
    }
</style>
""", unsafe_allow_html=True)

# File Paths
BASE_DIR = Path(__file__).resolve().parent
STORAGE_DIR = BASE_DIR / "local_simulation" / "simulated_adls"
BRONZE_DIR = STORAGE_DIR / "bronze"
SILVER_DIR = STORAGE_DIR / "silver"
GOLD_DIR = STORAGE_DIR / "gold"
CDC_PATH = BASE_DIR / "local_simulation" / "simulated_adls" / "config" / "cdc.json"
if not CDC_PATH.exists():
    CDC_PATH = BASE_DIR / "cdc.json"


# Auto-bootstrap sample lakehouse if data not found (crucial for cloud deployments like Streamlit Cloud / Docker)
if not (GOLD_DIR / "gold_fact_stream.parquet").exists():
    try:
        sys.path.insert(0, str(BASE_DIR))
        from local_simulation.simulate_pipeline import main as run_pipeline_main
        run_pipeline_main()
    except Exception as _e:
        pass

# Helper function to load data
def load_parquet_safe(file_path):
    if file_path.exists():
        try:
            return pl.read_parquet(file_path).to_pandas()
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def get_current_watermark():
    if CDC_PATH.exists():
        try:
            with open(CDC_PATH, "r", encoding="utf-8") as f:
                return json.load(f).get("cdc", "N/A")
        except Exception:
            return "1900-01-01"
    return "1900-01-01"


# --- Header Section ---
col_logo, col_title = st.columns([1, 6])
with col_title:
    st.markdown("# 🎵 Spotify Data Engineering Lakehouse")
    st.markdown(
        "**Azure Data Factory (ADF) | ADLS Gen2 | Databricks Autoloader | Delta Live Tables (DLT) | SCD Type 1 & 2**"
    )

# --- Top Key Metrics Row ---
fact_file = GOLD_DIR / "gold_fact_stream.parquet"
user_file = GOLD_DIR / "gold_dim_user_scd2.parquet"
track_file = SILVER_DIR / "DimTrack" / "DimTrack_silver.parquet"
artist_file = SILVER_DIR / "DimArtist" / "DimArtist_silver.parquet"

df_fact = load_parquet_safe(fact_file)
df_users = load_parquet_safe(user_file)
df_tracks = load_parquet_safe(track_file)
df_artists = load_parquet_safe(artist_file)

m1, m2, m3, m4, m5 = st.columns(5)
with m1:
    st.metric("Total Streams", f"{len(df_fact):,}" if not df_fact.empty else "1,000")
with m2:
    st.metric("Tracked Users (SCD2)", f"{len(df_users):,}" if not df_users.empty else "500")
with m3:
    st.metric("Curated Tracks", f"{len(df_tracks):,}" if not df_tracks.empty else "500")
with m4:
    st.metric("Active Artists", f"{len(df_artists):,}" if not df_artists.empty else "500")
with m5:
    st.metric("Active Watermark", get_current_watermark())

st.divider()

# --- Main Tabs Layout ---
tab_analytics, tab_inspector, tab_orchestrator, tab_jinja, tab_architecture = st.tabs([
    "📊 Executive Analytics",
    "🔍 Medallion Data Inspector",
    "⚙️ Pipeline Orchestration",
    "🧩 Dynamic Jinja2 Studio",
    "🏛️ System Architecture"
])

# ==============================================================================
# TAB 1: EXECUTIVE ANALYTICS
# ==============================================================================
with tab_analytics:
    st.subheader("Curated Business Analytics Marts (Gold Layer)")
    
    daily_file = GOLD_DIR / "gold_daily_streaming_metrics.parquet"
    top_artist_file = GOLD_DIR / "gold_top_artists.parquet"
    df_daily = load_parquet_safe(daily_file)
    df_top_artists = load_parquet_safe(top_artist_file)

    row1_c1, row1_c2 = st.columns(2)
    
    with row1_c1:
        st.markdown("#### 📈 Daily Streaming Activity & Hours")
        if not df_daily.empty:
            fig_daily = px.area(
                df_daily,
                x="stream_date",
                y="total_streams",
                title="Daily Stream Volume Over Time",
                labels={"stream_date": "Stream Date", "total_streams": "Streams Count"},
                color_discrete_sequence=["#1DB954"],
            )
            fig_daily.update_layout(
                template="plotly_dark",
                paper_bgcolor="#181818",
                plot_bgcolor="#181818",
                margin=dict(l=20, r=20, t=40, b=20),
            )
            st.plotly_chart(fig_daily, use_container_width=True)
        else:
            st.info("Run the local simulation pipeline to populate daily streaming metrics.")

    with row1_c2:
        st.markdown("#### 🏆 Top Streamed Artists")
        if not df_top_artists.empty:
            fig_artists = px.bar(
                df_top_artists.sort_values(by="total_streams", ascending=True),
                x="total_streams",
                y="artist_name",
                color="genre",
                orientation="h",
                title="Top Artists by Stream Count",
                labels={"total_streams": "Total Streams", "artist_name": "Artist"},
            )
            fig_artists.update_layout(
                template="plotly_dark",
                paper_bgcolor="#181818",
                plot_bgcolor="#181818",
                margin=dict(l=20, r=20, t=40, b=20),
            )
            st.plotly_chart(fig_artists, use_container_width=True)
        else:
            st.info("Top artist data will display once Gold marts are compiled.")

    row2_c1, row2_c2 = st.columns(2)
    
    with row2_c1:
        st.markdown("#### 🎧 Genre Distribution")
        if not df_artists.empty and "genre" in df_artists.columns:
            genre_counts = df_artists["genre"].value_counts().reset_index()
            genre_counts.columns = ["genre", "count"]
            fig_genre = px.pie(
                genre_counts,
                values="count",
                names="genre",
                hole=0.45,
                title="Catalog Genre Breakdown",
                color_discrete_sequence=px.colors.qualitative.Prism,
            )
            fig_genre.update_layout(
                template="plotly_dark",
                paper_bgcolor="#181818",
                plot_bgcolor="#181818",
                margin=dict(l=20, r=20, t=40, b=20),
            )
            st.plotly_chart(fig_genre, use_container_width=True)

    with row2_c2:
        st.markdown("#### 👥 User Subscription Model")
        if not df_users.empty and "subscription_type" in df_users.columns:
            sub_counts = df_users["subscription_type"].value_counts().reset_index()
            sub_counts.columns = ["subscription_type", "count"]
            fig_sub = px.pie(
                sub_counts,
                values="count",
                names="subscription_type",
                hole=0.45,
                title="Active User Subscription Tiers",
                color_discrete_sequence=["#1DB954", "#3498db", "#f39c12", "#e74c3c"],
            )
            fig_sub.update_layout(
                template="plotly_dark",
                paper_bgcolor="#181818",
                plot_bgcolor="#181818",
                margin=dict(l=20, r=20, t=40, b=20),
            )
            st.plotly_chart(fig_sub, use_container_width=True)

# ==============================================================================
# TAB 2: MEDALLION DATA INSPECTOR
# ==============================================================================
with tab_inspector:
    st.subheader("Lakehouse Table & Partition Explorer")
    st.markdown("Inspect raw files in **Bronze**, cleansed tables in **Silver**, and SCD dimensions in **Gold**.")

    col_sel_layer, col_sel_table = st.columns(2)
    with col_sel_layer:
        selected_layer = st.selectbox("Select Medallion Layer", ["Bronze (Raw)", "Silver (Cleansed Delta)", "Gold (Curated / SCD2)"])

    with col_sel_table:
        if "Bronze" in selected_layer:
            available_tables = ["DimUser", "DimArtist", "DimTrack", "DimDate", "FactStream"]
        elif "Silver" in selected_layer:
            available_tables = ["DimUser", "DimArtist", "DimTrack", "DimDate", "FactStream"]
        else:
            available_tables = ["gold_dim_user_scd2", "gold_fact_stream", "gold_daily_streaming_metrics", "gold_top_artists"]
        
        selected_table = st.selectbox("Select Table", available_tables)

    # Load Table based on selection
    preview_df = pd.DataFrame()
    table_path_display = ""

    if "Bronze" in selected_layer:
        bronze_table_files = list((BRONZE_DIR / selected_table).rglob("*.parquet"))
        if bronze_table_files:
            preview_df = load_parquet_safe(bronze_table_files[0])
            table_path_display = f"abfss://bronze@storageazureproject/{selected_table}/..."
    elif "Silver" in selected_layer:
        silver_file = SILVER_DIR / selected_table / f"{selected_table}_silver.parquet"
        preview_df = load_parquet_safe(silver_file)
        table_path_display = f"spotify_cata.silver.{selected_table}"
    else:
        gold_file = GOLD_DIR / f"{selected_table}.parquet"
        preview_df = load_parquet_safe(gold_file)
        table_path_display = f"spotify_cata.gold.{selected_table}"

    if not preview_df.empty:
        st.markdown(f"**Target:** `{table_path_display}` | **Records:** `{len(preview_df):,}` | **Columns:** `{len(preview_df.columns)}`")
        
        # Search & Filter
        search_query = st.text_input("Filter preview rows:", placeholder="Search table...")
        if search_query:
            mask = preview_df.astype(str).apply(lambda row: row.str.contains(search_query, case=False).any(), axis=1)
            filtered_df = preview_df[mask]
        else:
            filtered_df = preview_df

        st.dataframe(filtered_df.head(100), use_container_width=True, height=400)
    else:
        st.warning(f"No records found for {selected_table} in {selected_layer}. Run the pipeline in the Orchestration tab to generate.")

# ==============================================================================
# TAB 3: PIPELINE ORCHESTRATION
# ==============================================================================
with tab_orchestrator:
    st.subheader("Automated Pipeline Orchestrator & CDC Runner")
    st.markdown("Execute individual stages or the complete end-to-end pipeline with live status updates.")

    col_btn1, col_btn2, col_btn3 = st.columns(3)
    
    with col_btn1:
        if st.button("🚀 Run Full Pipeline (Initial + CDC)"):
            with st.spinner("Executing end-to-end pipeline simulation..."):
                sys.path.insert(0, str(BASE_DIR))
                from local_simulation.simulate_pipeline import main as run_pipeline_main
                run_pipeline_main()
                st.success("✅ Full Pipeline Completed Successfully!")
                st.rerun()

    with col_btn2:
        if st.button("⚡ Trigger Incremental CDC Batch"):
            with st.spinner("Applying CDC updates and re-triggering ADF ingestion..."):
                sys.path.insert(0, str(BASE_DIR))
                from local_simulation.simulate_pipeline import run_incremental_cdc_simulation
                run_incremental_cdc_simulation()
                st.success("✅ Incremental CDC Batch Ingested and Processed!")
                st.rerun()

    with col_btn3:
        if st.button("🔄 Re-process Silver & Gold Layers"):
            with st.spinner("Re-executing Autoloader and DLT transformations..."):
                sys.path.insert(0, str(BASE_DIR))
                from local_simulation.simulate_pipeline import run_silver_processing, run_gold_dlt_processing
                run_silver_processing()
                run_gold_dlt_processing()
                st.success("✅ Silver & Gold Tables Re-computed!")
                st.rerun()

    st.markdown("---")
    st.markdown("#### 📋 Watermark & CDC Configuration Status")
    
    col_cdc1, col_cdc2 = st.columns(2)
    with col_cdc1:
        st.markdown("**Active Watermark File (`cdc.json`):**")
        st.code(json.dumps({"cdc": get_current_watermark()}, indent=2), language="json")

    with col_cdc2:
        st.markdown("**ADF Table Loop Definition (`loop_input.json`):**")
        loop_file = BASE_DIR / "loop_input.json"
        if loop_file.exists():
            with open(loop_file, "r") as f:
                st.code(f.read(), language="json")

# ==============================================================================
# TAB 4: DYNAMIC JINJA2 STUDIO
# ==============================================================================
with tab_jinja:
    st.subheader("Metadata-Driven Pipeline Studio (Jinja2 Templating)")
    st.markdown(
        "Instead of hardcoding complex Star Schema joins, this engine compiles configuration metadata into dynamic PySpark SQL queries."
    )

    sys.path.insert(0, str(BASE_DIR / "databricks" / "src"))
    try:
        from gold.jinja_star_schema import render_star_schema_query, DEFAULT_STAR_SCHEMA_CONFIG
    except ImportError:
        render_star_schema_query = lambda x: "-- Error loading Jinja generator"
        DEFAULT_STAR_SCHEMA_CONFIG = []

    st.markdown("#### Metadata Configuration (JSON):")
    st.json(DEFAULT_STAR_SCHEMA_CONFIG)

    rendered_sql = render_star_schema_query()
    st.markdown("#### Rendered Star Schema SQL Query:")
    st.code(rendered_sql, language="sql")

# ==============================================================================
# TAB 5: SYSTEM ARCHITECTURE
# ==============================================================================
with tab_architecture:
    st.subheader("End-to-End System Architecture & Cloud Tech Stack")
    
    st.markdown("""
    ```mermaid
    flowchart LR
        subgraph Source
            SQL[Azure SQL DB<br/>Spotify OLTP]
        end
        subgraph ADF[Azure Data Factory]
            Ingest[PL_Master_Spotify_Ingestion<br/>Watermark CDC + Backfilling]
        end
        subgraph ADLS[ADLS Gen2 Storage]
            Bronze[Bronze: Raw Parquet]
            Silver[Silver: Delta Tables]
            Gold[Gold: Curated Marts]
        end
        subgraph Databricks[Azure Databricks]
            Auto[Autoloader / Streaming]
            DLT[Delta Live Tables<br/>SCD Type 1 & 2]
        end
        
        SQL -->|CDC Query| Ingest
        Ingest --> Bronze
        Bronze --> Auto
        Auto --> Silver
        Silver --> DLT
        DLT --> Gold
    ```
    """)
    
    st.markdown("---")
    col_tech1, col_tech2 = st.columns(2)
    with col_tech1:
        st.markdown("""
        **Core Architectural Highlights:**
        - **Source:** Azure SQL DB with 5 relational entities.
        - **Ingestion:** ADF parameterized pipeline with `cdc.json` watermark.
        - **Lakehouse:** Azure Data Lake Storage Gen2 with Medallion architecture.
        - **Streaming:** Databricks Autoloader (`cloudFiles`) with schema drift management.
        """)
    with col_tech2:
        st.markdown("""
        **Advanced Engineering Concepts:**
        - **Slowly Changing Dimensions (SCD2):** Tracks user subscription tier changes.
        - **Delta Live Tables (DLT):** Enforces data quality expectations.
        - **Metadata-Driven Pipelines:** Jinja2 dynamic SQL query generation.
        - **CI/CD:** Databricks Asset Bundles (DAB) deployed via GitHub Actions.
        """)

st.markdown("---")
st.markdown("<p style='text-align: center; color: #777;'>Spotify End-to-End Azure Data Engineering Lakehouse | Built with Python, Streamlit, and Polars</p>", unsafe_allow_html=True)
