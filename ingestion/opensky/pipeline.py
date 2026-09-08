"""
Orchestration: the persistent poll loop. This is the only module that knows
about "streaming behavior" (cadence, retry/backoff) -- swapping the sink or
adding a second source/bounding box means editing this file, not extract.py
or load.py.

Run as: python -m ingestion.opensky.pipeline
Intended to run under systemd or Docker with an always-restart policy --
see deploy/.
"""

import sys
import time
import logging
import requests
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from . import config
from .extract import fetch_states
from .load import ensure_table, load_batch

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("opensky_bq.pipeline")


def poll_loop():
    client = bigquery.Client(project=config.BQ_PROJECT)
    dataset_ref = f"{client.project}.{config.BQ_DATASET}"
    table_ref = f"{dataset_ref}.{config.BQ_TABLE}"

    client.create_dataset(dataset_ref, exists_ok=True)
    ensure_table(client, table_ref)

    session = requests.Session()
    backoff = config.POLL_INTERVAL_SECONDS

    log.info(
        f"Starting poll loop: interval={config.POLL_INTERVAL_SECONDS}s, "
        f"table={table_ref}, partition_expiration={config.PARTITION_EXPIRATION_DAYS}d"
    )

    while True:
        cycle_start = time.monotonic()
        try:
            df = fetch_states(session)
            if df is None:
                log.info("No states returned this cycle.")
            else:
                load_batch(client, table_ref, df)
            backoff = config.POLL_INTERVAL_SECONDS  # reset after a good cycle

        except (requests.exceptions.RequestException, GoogleAPIError) as e:
            log.warning(f"Cycle failed: {e}. Backing off {backoff}s.")
            time.sleep(backoff)
            backoff = min(backoff * 2, config.MAX_BACKOFF_SECONDS)
            continue

        elapsed = time.monotonic() - cycle_start
        time.sleep(max(0, config.POLL_INTERVAL_SECONDS - elapsed))


def main():
    try:
        poll_loop()
    except KeyboardInterrupt:
        log.info("Shutting down.")
        sys.exit(0)


if __name__ == "__main__":
    main()
