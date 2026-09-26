"""
Metadata-Driven Pipeline with PySpark and Jinja2 Templating.
Dynamically constructs Star Schema analytical queries based on configuration dictionaries.
"""
from jinja2 import Template
from typing import List, Dict, Any


DEFAULT_STAR_SCHEMA_CONFIG = [
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

QUERY_TEMPLATE = """
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


def render_star_schema_query(parameters: List[Dict[str, Any]] = None) -> str:
    """
    Renders a dynamic SQL query using Jinja2 templating based on metadata parameters.
    """
    params = parameters or DEFAULT_STAR_SCHEMA_CONFIG
    template = Template(QUERY_TEMPLATE)
    rendered_sql = template.render(parameters=params)
    return "\n".join([line.strip() for line in rendered_sql.strip().split("\n") if line.strip()])


def execute_metadata_query(spark_session, parameters: List[Dict[str, Any]] = None):
    """
    Executes the rendered Jinja SQL query on a PySpark session.
    """
    query = render_star_schema_query(parameters)
    print("Generated Metadata-Driven Query:\n", query)
    return spark_session.sql(query)


if __name__ == "__main__":
    generated_sql = render_star_schema_query()
    print("Rendered Dynamic Star Schema Query:\n")
    print(generated_sql)
