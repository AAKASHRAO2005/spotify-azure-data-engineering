# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Metadata-Driven Pipelines with PySpark and Jinja2
# MAGIC This notebook demonstrates how to construct dynamic star-schema SQL queries
# MAGIC based on table configuration dictionaries using Jinja2 templating.

# COMMAND ----------
# MAGIC %pip install jinja2

# COMMAND ----------
from jinja2 import Template

# Define metadata-driven query configuration
parameters = [
    {
        "table": "spotify_cata.silver.FactStream",
        "alias": "factstream",
        "cols": "factstream.stream_id, factstream.listen_duration, factstream.device_type, factstream.stream_timestamp"
    },
    {
        "table": "spotify_cata.silver.DimUser",
        "alias": "dimuser",
        "cols": "dimuser.user_id, dimuser.user_name, dimuser.country, dimuser.subscription_type",
        "condition": "factstream.user_id = dimuser.user_id"
    },
    {
        "table": "spotify_cata.silver.DimTrack",
        "alias": "dimtrack",
        "cols": "dimtrack.track_id, dimtrack.track_name, dimtrack.album_name, dimtrack.duration_sec, dimtrack.durationFlag",
        "condition": "factstream.track_id = dimtrack.track_id"
    },
    {
        "table": "spotify_cata.silver.DimArtist",
        "alias": "dimartist",
        "cols": "dimartist.artist_id, dimartist.artist_name, dimartist.genre",
        "condition": "dimtrack.artist_id = dimartist.artist_id"
    }
]

# COMMAND ----------
# Jinja2 Dynamic SQL Query Template
query_template = """
SELECT 
    {% for param in parameters %}
        {{ param.cols }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM 
    {% for param in parameters %}
        {% if loop.first %}
            {{ param['table'] }} AS {{ param['alias'] }}
        {% endif %}
    {% endfor %}
    {% for param in parameters %}
        {% if not loop.first %}
        LEFT JOIN 
            {{ param['table'] }} AS {{ param['alias'] }} 
        ON 
            {{ param['condition'] }}
        {% endif %}
    {% endfor %}
"""

# COMMAND ----------
jinja_sql_str = Template(query_template)
rendered_query = jinja_sql_str.render(parameters=parameters)
print("=== GENERATED METADATA-DRIVEN SQL QUERY ===")
print(rendered_query)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Execute Rendered SQL in PySpark
# COMMAND ----------
df_star_schema = spark.sql(rendered_query)
display(df_star_schema)
