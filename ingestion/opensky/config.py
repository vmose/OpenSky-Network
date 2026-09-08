"""
Central configuration for the OpenSky -> BigQuery pipeline.

Everything here is env-driven so the same code runs unchanged under
systemd, Docker, or a plain `python -m` invocation on a laptop.

Note: POLL_INTERVAL_SECONDS is intentionally NOT tied to whether OpenSky
credentials are set. Auth raises your API rate-limit ceiling (useful for
reliability), but the poll interval is a cost/storage decision, not a
rate-limit decision -- conflating the two is what silently reintroduces
the free-tier-blowout problem this pipeline was built to avoid.
"""

import os

# --- OpenSky source ---------------------------------------------------
OPENSKY_USER = os.getenv("OPENSKY_USER", "")
OPENSKY_PASSWORD = os.getenv("OPENSKY_PASSWORD", "")
API_URL = "https://opensky-network.org/api/states/all"

# Bounding box: southern Africa/Atlantic -> southern Europe/Central Asia
# -> Singapore/western Indonesia. Kept fixed per project requirements.
BOUNDING_BOX = {
    "lamin": -35.0,
    "lomin": -20.0,
    "lamax": 40.0,
    "lomax": 104.0,
}

# --- Poll / retry behavior ---------------------------------------------
# 5 min default keeps steady-state BigQuery storage around ~3.2 GB with a
# 14-day partition expiration -- comfortably under the 10 GB free tier.
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", 300))
MAX_BACKOFF_SECONDS = int(os.getenv("MAX_BACKOFF_SECONDS", 1800))

# --- BigQuery sink -------------------------------------------------------
BQ_PROJECT = os.getenv("BQ_PROJECT")  # None -> use ADC's default project
BQ_DATASET = os.getenv("BQ_DATASET", "opensky")
BQ_TABLE = os.getenv("BQ_TABLE", "live_flights")

# Partitions older than this are auto-dropped by BigQuery, bounding
# steady-state storage regardless of how long the process runs.
PARTITION_EXPIRATION_DAYS = int(os.getenv("PARTITION_EXPIRATION_DAYS", 14))
