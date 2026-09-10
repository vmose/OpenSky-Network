# OpenSky → BigQuery Ingestion Pipeline

A lightweight, always-on ingestion pipeline that polls the [OpenSky
Network](https://opensky-network.org/) `/states/all` endpoint for live
flight state vectors over a fixed bounding box and loads them into
BigQuery, without requiring a GCP billing account.

## How it works

There's no push/streaming feed available from OpenSky — it's a polling
REST API. This pipeline runs as a **persistent process** that polls on a
fixed interval and appends each batch to BigQuery via a load job:

```
[fetch_states] --(DataFrame)--> [load_batch] --(load job)--> BigQuery
      |                                                          |
  poll loop, retry/backoff                          day-partitioned table,
  (pipeline.py)                                      partition expiration
```

This is a **micro-batch** pipeline, not sub-second streaming — latency is
capped by the poll interval, and true low-latency ingestion (BigQuery's
Storage Write API) requires a billing account and bills per row regardless
of free tier. Load jobs, by contrast, are free; only the resulting stored
bytes count against BigQuery's 10 GB/month free storage tier.

## Why storage doesn't grow unbounded

The target table is partitioned by day on `ingested_at`, with a partition
expiration set at creation time. Old partitions are dropped automatically,
so storage plateaus at roughly:

```
steady-state storage ≈ daily ingest rate × PARTITION_EXPIRATION_DAYS
```

Defaults (5-minute poll, 14-day expiration) land around **~3.2 GB**
steady state — comfortably under the free tier. See `.env.example` to
adjust either value; steeper polling or longer retention should be
checked against the free tier math above before deploying.

## Project structure

```
ingestion/opensky/
├── config.py     # all tunables, env-driven
├── schema.py     # BQ_SCHEMA / COLUMNS — shared contract between extract & load
├── extract.py    # fetch_states() — pure HTTP + parsing, no sink knowledge
├── load.py       # ensure_table(), load_batch() — BigQuery-specific sink
└── pipeline.py   # poll_loop() — the persistent orchestration loop

deploy/
├── systemd/opensky-ingest.service   # run as a native systemd service
└── docker/                          # run as a container instead
    ├── Dockerfile
    └── docker-compose.yml
```

`extract.py` and `load.py` are deliberately decoupled: swapping the sink
(e.g. to Postgres or a message broker) or adding a second data source only
touches one file each, not the orchestration logic in `pipeline.py`.

## Setup

1. **Clone and install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment**

   ```bash
   cp .env.example .env
   # edit .env: set BQ_PROJECT, optionally OPENSKY_USER/PASSWORD, etc.
   ```

3. **Authenticate to GCP** — either:

   ```bash
   gcloud auth application-default login
   ```

   or set `GOOGLE_APPLICATION_CREDENTIALS` in `.env` to a service account
   key path.

4. **Run locally**

   ```bash
   python -m ingestion.opensky.pipeline
   ```

   The target dataset/table is created automatically on first run,
   partitioned and with expiration applied at creation time.

## Deployment

Pick **one** of the following — they run the same process, just managed
differently.

### systemd (bare VM)

```bash
sudo useradd -r -s /bin/false opensky
sudo mkdir -p /opt/opensky-pipeline
sudo cp -r . /opt/opensky-pipeline
cd /opt/opensky-pipeline
python3 -m venv venv && venv/bin/pip install -r requirements.txt
cp .env.example .env   # then edit
sudo cp deploy/systemd/opensky-ingest.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now opensky-ingest
```

Check status/logs:

```bash
systemctl status opensky-ingest
journalctl -u opensky-ingest -f
```

### Docker

```bash
cp .env.example .env   # then edit
# place your service account key at deploy/docker/service-account.json
docker compose -f deploy/docker/docker-compose.yml up -d
```

## Configuration reference

All settings are environment variables — see `.env.example` for the full
list and defaults. Notable ones:

| Variable | Default | Notes |
|---|---|---|
| `POLL_INTERVAL_SECONDS` | `300` | Cost/storage decision — intentionally not tied to auth status |
| `PARTITION_EXPIRATION_DAYS` | `14` | Controls steady-state storage; see math above |
| `MAX_BACKOFF_SECONDS` | `1800` | Ceiling for exponential backoff on repeated failures |
| `BQ_PROJECT` / `BQ_DATASET` / `BQ_TABLE` | — / `opensky` / `live_flights` | BigQuery target |

## Known limitations

- **No row-level data quality checks yet.** Invalid or implausible values
  from OpenSky (e.g. out-of-range coordinates, negative velocity) load
  as-is; only hard type mismatches would fail a load job.
- **No dedup on retry.** If a load job succeeds but the process doesn't
  record that before a retry fires, the same batch can be appended twice.
  Harmless for storage size given partition expiration, but can produce
  duplicate rows in a given window.
- **Single source, single sink.** No orchestrator (Airflow) or message
  broker is used by design — this is the right amount of infrastructure
  for one polling source and one destination. Revisit if a second
  consumer or data source is added.
