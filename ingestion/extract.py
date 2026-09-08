import os
import time
import asyncio
import logging
import requests
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("opensky_stream")

OPENSKY_USER = os.getenv("OPENSKY_USER", "")
OPENSKY_PASSWORD = os.getenv("OPENSKY_PASSWORD", "")
API_URL = "https://opensky-network.org/api/states/all"

BOUNDING_BOX = {
    "lamin": -35.0, "lomin": -20.0,
    "lamax": 40.0, "lomax": 104.0,
}

# Respect OpenSky's published limits: 5s authenticated / 10s anonymous for a bbox query.
POLL_INTERVAL = 5 if (OPENSKY_USER and OPENSKY_PASSWORD) else 300
MAX_BACKOFF = 1800

COLUMNS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source",
]

OUTPUT_ROOT = Path("live_flights_lake")


def fetch_states(session: requests.Session) -> pd.DataFrame | None:
    params = dict(BOUNDING_BOX) if BOUNDING_BOX else {}
    auth = (OPENSKY_USER, OPENSKY_PASSWORD) if OPENSKY_USER and OPENSKY_PASSWORD else None

    resp = session.get(API_URL, params=params, auth=auth, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    states = data.get("states", [])
    if not states:
        return None

    df = pd.DataFrame(states, columns=COLUMNS)
    df["callsign"] = df["callsign"].str.strip()
    df["time_position"] = pd.to_datetime(df["time_position"], unit="s", errors="coerce", utc=True)
    df["last_contact"] = pd.to_datetime(df["last_contact"], unit="s", errors="coerce", utc=True)
    df["ingested_at"] = datetime.now(timezone.utc)
    return df


def write_partition(df: pd.DataFrame) -> None:
    """Append this cycle's batch to an hour-partitioned Parquet dataset."""
    now = datetime.now(timezone.utc)
    partition = OUTPUT_ROOT / f"year={now:%Y}" / f"month={now:%m}" / f"day={now:%d}" / f"hour={now:%H}"
    partition.mkdir(parents=True, exist_ok=True)
    fname = partition / f"batch_{now:%Y%m%dT%H%M%S}.parquet"
    df.to_parquet(fname, index=False)


async def poll_loop():
    session = requests.Session()
    backoff = POLL_INTERVAL
    seen_last_contact: dict[str, int] = {}  # icao24 -> last seen epoch, for optional dedup

    while True:
        cycle_start = time.monotonic()
        try:
            df = fetch_states(session)
            if df is None:
                log.info("No states returned this cycle.")
            else:
                # Optional: drop rows we've already emitted with the same last_contact
                new_mask = df.apply(
                    lambda r: seen_last_contact.get(r["icao24"]) != r["last_contact"], axis=1
                )
                new_rows = df[new_mask]
                for _, r in new_rows.iterrows():
                    seen_last_contact[r["icao24"]] = r["last_contact"]

                if not new_rows.empty:
                    write_partition(new_rows)
                    log.info(f"Wrote {len(new_rows)} new/updated rows.")
                else:
                    log.info("No new state changes since last cycle.")

            backoff = POLL_INTERVAL  # reset on success

        except requests.exceptions.RequestException as e:
            log.warning(f"Fetch failed: {e}. Backing off to {backoff}s.")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
            continue

        elapsed = time.monotonic() - cycle_start
        await asyncio.sleep(max(0, POLL_INTERVAL - elapsed))


if __name__ == "__main__":
    try:
        asyncio.run(poll_loop())
    except KeyboardInterrupt:
        log.info("Shutting down.") 
