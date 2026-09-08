"""
Extraction: fetch + parse OpenSky state vectors. Deliberately sink-agnostic
-- if a second sink (Kafka, Postgres, ...) is ever added, this module is
reused unchanged.
"""

import requests
import pandas as pd

from . import config
from .schema import COLUMNS


def fetch_states(session: requests.Session):
    """Fetch and parse current state vectors for config.BOUNDING_BOX.

    Returns a DataFrame, or None if no states were returned this cycle.
    Raises requests.exceptions.RequestException on transport/HTTP failure
    -- the caller (pipeline.py) owns retry/backoff policy.
    """
    params = dict(config.BOUNDING_BOX)
    auth = (
        (config.OPENSKY_USER, config.OPENSKY_PASSWORD)
        if config.OPENSKY_USER and config.OPENSKY_PASSWORD
        else None
    )

    resp = session.get(config.API_URL, params=params, auth=auth, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    states = data.get("states", [])
    if not states:
        return None

    df = pd.DataFrame(states, columns=COLUMNS)
    df = df.drop(columns=["sensors"])

    df["callsign"] = df["callsign"].str.strip()
    df["time_position"] = pd.to_datetime(df["time_position"], unit="s", errors="coerce", utc=True)
    df["last_contact"] = pd.to_datetime(df["last_contact"], unit="s", errors="coerce", utc=True)
    df["ingested_at"] = pd.Timestamp.now(tz="UTC")

    return df