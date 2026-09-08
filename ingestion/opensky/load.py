"""
Sink: BigQuery table management and load jobs. This is the one module you'd
swap or extend if you ever add a second destination.
"""

import logging
import pandas as pd
from google.cloud import bigquery

from . import config
from .schema import BQ_SCHEMA

log = logging.getLogger("opensky_bq.load")


def ensure_table(client: bigquery.Client, table_ref: str) -> None:
    """Create the target table if missing: partitioned by day on
    ingested_at, with expiration so old partitions are dropped
    automatically and steady-state storage stays bounded."""
    try:
        client.get_table(table_ref)
        return
    except Exception:
        pass  # table doesn't exist yet -> create it below

    table = bigquery.Table(table_ref, schema=BQ_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ingested_at",
        expiration_ms=config.PARTITION_EXPIRATION_DAYS * 24 * 60 * 60 * 1000,
    )
    client.create_table(table)
    log.info(
        f"Created {table_ref}, partitioned by day on 'ingested_at', "
        f"partition expiration = {config.PARTITION_EXPIRATION_DAYS} days."
    )


def load_batch(client: bigquery.Client, table_ref: str, df: pd.DataFrame) -> None:
    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # wait for completion; raises on failure
    log.info(f"Loaded {len(df)} rows into {table_ref} (job {job.job_id}).")
