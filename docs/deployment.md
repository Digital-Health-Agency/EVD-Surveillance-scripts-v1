# Deployment

The stack deploys with Docker Compose: **one image, two containers** — a webserver (UI) and a daemon (schedules + run execution) — sharing SQLite state through a volume. No Postgres for Dagster's own state, no per-run containers, no gRPC code server.

The one deployment serves **two code locations** ([`deploy/workspace.yaml`](../deploy/workspace.yaml)): `datasources` (dlt loaders, APIs → MinIO) and `evd_warehouse` (MinIO → Postgres bronze → dbt silver/marts/gold, formerly the separate `EVD-Warehouse-ETL` deployment). All schedules and sensors of both run on the one daemon and share the one run queue. This layout fits the project's scale (a handful of sources, daily schedules); the [scaling path](#when-to-outgrow-this-setup) is documented below.

```
┌─────────────────────────┐   ┌─────────────────────────┐
│  webserver              │   │  daemon                 │
│  dagster-webserver :3000│   │  dagster-daemon         │
│  UI, queues runs        │   │  fires schedules,       │
│                         │   │  executes runs (subproc)│
└───────────┬─────────────┘   └───────────┬─────────────┘
     image: evd-dagster            image: evd-dagster
            └──────────┬──────────────────┘
            dagster_data volume (SQLite state)
                       │
             MinIO (runs separately)
```

## Files

| File | Role | Changes require |
| --- | --- | --- |
| [`Dockerfile`](../Dockerfile) | One image: Dagster + this project + config | rebuild |
| [`docker-compose.yml`](../docker-compose.yml) | The two services, volume, env wiring | `docker compose up -d` |
| [`deploy/dagster.yaml`](../deploy/dagster.yaml) | Dagster instance config (queue, run monitoring) — baked into the image | rebuild |
| [`deploy/workspace.yaml`](../deploy/workspace.yaml) | Points Dagster at `datasources.definitions` — baked into the image | rebuild |
| [`.env.example`](../.env.example) | Template for `.env` (ports, MinIO connection) | — |
| [`.dockerignore`](../.dockerignore) | Keeps `.dlt/secrets.toml`, `.venv`, `.git` out of the image | — |

**Config is baked into the image on purpose.** Named volumes copy image files only on *first* mount, so config placed in a volume goes stale and silently shadows later changes. Here the volume holds only run data (`/opt/dagster/data`); `dagster.yaml` and `workspace.yaml` live in the image, so every rebuild deploys the current config.

## First deployment

Prerequisites: Docker with Compose, and a reachable MinIO with the target bucket created.

```bash
git clone <repo> && cd evd-surveillance-scripts
cp .env.example .env         # then edit: MinIO endpoint, credentials, bucket
docker compose up -d --build
```

Open http://localhost:3000 (or `DAGSTER_WEB_PORT`). Verify:

```bash
docker compose ps                          # both services Up
docker compose logs daemon --since 2m      # no errors; schedule ticks appear
curl -s localhost:3000/server_info         # webserver + dagster versions
```

Then turn on the schedule(s) in the UI (Automation tab) — schedules start out stopped.

## Configuration

All runtime settings flow through environment variables set in `docker-compose.yml`, overridable via `.env`:

| Variable | Default | Meaning |
| --- | --- | --- |
| `DAGSTER_WEB_PORT` | `3000` | Host port for the UI |
| `DAGSTER_WEB_BIND` | `0.0.0.0` | Host interface the UI port binds to — `127.0.0.1` behind a reverse proxy |
| `DAGSTER_PATH_PREFIX` | empty (served at `/`) | URL path the UI lives under behind a proxy, e.g. `/dagster` (no trailing slash) |
| `MINIO_ENDPOINT_URL` | `http://host.docker.internal:9000` | MinIO server, as reachable *from inside the containers* |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | `minioadmin` | MinIO credentials — override outside local dev |
| `BUCKET_URL` | `s3://evd/` | Target bucket (name only; endpoint stays in the credential vars) |
| `MINIO_ENDPOINT` / `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | the three MinIO values above | MinIO the warehouse ETL reads from — only set to override |
| `MINIO_BUCKET` | `evd` | Bucket the warehouse ETL reads — name only, must match `BUCKET_URL` |
| `PG_HOST` / `PG_PORT` / `PG_DBNAME` | required / `5432` / `evd_dwh` | Warehouse Postgres (bronze/silver/marts/gold), as reachable from inside the containers |
| `PG_USER` / `PG_PASSWORD` | required | Warehouse Postgres credentials |

Secrets in deployment are env vars only — the containers have no `.dlt/secrets.toml` (excluded by `.dockerignore`). Per-source API tokens follow the same pattern: add e.g. `DATASOURCES__MDHARURA__API_TOKEN` to both services' `environment` blocks (dlt maps `__`-separated env vars to config paths — see [pipelines-and-destinations.md](pipelines-and-destinations.md#configuration--secrets)).

Instance-level behavior (run queue size, run timeout) lives in [`deploy/dagster.yaml`](../deploy/dagster.yaml):

- `QueuedRunCoordinator`, `max_concurrent_runs: 4` — the daemon launches all runs, so the UI stays responsive and a schedule tick can't stampede the source APIs.
- `run_monitoring.max_runtime_seconds: 14400` — runs are killed after 4 h; raise before very large backfills.

## Migrating from two deployments (one-time)

Before the merge, the warehouse ETL ran as its own Dagster (`EVD-Warehouse-ETL` repo, port 3001, `evddwh-dwh` compose project). Cut over in this order:

1. Add the `PG_*` values (from the old warehouse `.env`) to this repo's `.env`. If the old warehouse `MINIO_*` values differ from this repo's MinIO settings, set `MINIO_ENDPOINT`/`MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` too.
2. **Stop the old warehouse stack** — in the old checkout: `docker compose down` (no `-v` if you want to keep its run history volume around). If both run, `ingest_job` fires twice at 01:00 UTC.
3. Deploy this repo: `docker compose up -d --build`, then check both code locations are green under *Deployment → Code locations* in the UI.

Warehouse run history from the old instance does not carry over (it lived in that stack's own SQLite volume); bronze/silver/gold data in Postgres and the MinIO files are untouched. Schedules start as `RUNNING` (`default_status`) in both locations.

## Deploying changes

```bash
git pull
docker compose up -d --build
```

That rebuilds the image (dependency layer is cached unless `uv.lock` changed) and recreates both containers. Two operational notes:

- **In-flight runs die on redeploy** (they're subprocesses of the daemon container). Deploy outside schedule windows — the syncs and the warehouse ingest fire between 01:00 and 08:00 UTC, with dbt following each ingest — and re-launch any interrupted run from the UI; run monitoring will have marked it failed.
- Run history, schedule state, and incremental cursors survive: Dagster state is in the `dagster_data` volume, and dlt cursors live in the destination bucket itself.

## Operations

```bash
docker compose logs -f daemon          # run execution + schedule logs
docker compose logs -f webserver       # UI/API logs
docker compose exec daemon ls /opt/dagster/data/storage    # SQLite files
docker compose down                    # stop (volume preserved)
docker compose down -v                 # stop AND delete run history — irreversible
```

**Backup**: everything worth keeping is the `dagster_data` volume (run history), the MinIO bucket (the actual data + dlt state), and the warehouse Postgres. The volume is disposable if you can live without run history — the pipelines and cursors rebuild from the bucket.

## Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| Run fails with `Temporary failure in name resolution` right after startup | Container network not ready yet — transient; re-launch the run |
| UI shows code location error after deploy | `docker compose logs webserver` — usually an import error in a source; fix and redeploy. `evd_warehouse` alone failing usually means a missing/wrong `PG_*` or `MINIO_*` var, or the dbt manifest failed to build |
| Config change has no effect | `dagster.yaml`/`workspace.yaml` are baked into the image — rebuild with `--build` |
| `database is locked` errors | SQLite write contention — time to [move to Postgres](#when-to-outgrow-this-setup) |
| Schedule not firing | Check it's started (Automation tab) and the daemon container is healthy |
| Can't reach MinIO from containers | `MINIO_ENDPOINT_URL` must be reachable from *inside* Docker — `localhost` won't work; use `host.docker.internal` or a real hostname |

## When to outgrow this setup

Move to the next tier when run volume, team size, or reliability needs grow:

1. **Postgres storage** (first step): add a `db` service, replace the `storage:` block in `dagster.yaml` with `dagster-postgres` run/event/schedule storage, add `dagster-postgres` to dependencies. Removes the SQLite single-writer limit.
2. **Separate code location** (second step): move user code into its own gRPC server container (`dagster api grpc`), point `workspace.yaml` at it, and optionally switch to `DockerRunLauncher` for per-run container isolation. This is Dagster's standard multi-container reference architecture — deploys then no longer restart the webserver/daemon or interrupt the queue.
