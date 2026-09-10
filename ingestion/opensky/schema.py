"""
Single source of truth for the OpenSky state-vector shape, shared by
extract.py (parsing) and load.py (BigQuery table + load jobs) so the two
can't silently drift apart.
"""

from google.cloud import bigquery

# Raw column order as returned by OpenSky's /states/all endpoint.
COLUMNS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source",
]

# BigQuery target schema. "sensors" is dropped in extract.py before load --
# it's a rarely-populated REPEATED field, not worth the schema complexity.
BQ_SCHEMA = [
    bigquery.SchemaField("icao24", "STRING"),
    bigquery.SchemaField("callsign", "STRING"),
    bigquery.SchemaField("origin_country", "STRING"),
    bigquery.SchemaField("time_position", "TIMESTAMP"),
    bigquery.SchemaField("last_contact", "TIMESTAMP"),
    bigquery.SchemaField("longitude", "FLOAT"),
    bigquery.SchemaField("latitude", "FLOAT"),
    bigquery.SchemaField("baro_altitude", "FLOAT"),
    bigquery.SchemaField("on_ground", "BOOLEAN"),
    bigquery.SchemaField("velocity", "FLOAT"),
    bigquery.SchemaField("true_track", "FLOAT"),
    bigquery.SchemaField("vertical_rate", "FLOAT"),
    bigquery.SchemaField("geo_altitude", "FLOAT"),
    bigquery.SchemaField("squawk", "STRING"),
    bigquery.SchemaField("spi", "BOOLEAN"),
    bigquery.SchemaField("position_source", "INTEGER"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
]
