# Single image for both dagster-webserver and dagster-daemon — docker-compose.yml
# runs it as two containers sharing $DAGSTER_HOME (SQLite storage) via a volume.
# It carries both code locations (deploy/workspace.yaml): the dlt ingestion
# sources (src/datasources) and the warehouse ETL (orchestration/evd_orchestration
# + the dbt project in transform/).

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

ENV DAGSTER_HOME=/opt/dagster/home \
    UV_COMPILE_BYTECODE=1

WORKDIR /opt/dagster/app

# dependency layer first so code changes don't re-resolve packages
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY orchestration ./orchestration
COPY transform ./transform
COPY infra ./infra
COPY deploy/workspace.yaml ./
COPY .dlt/config.toml ./.dlt/config.toml
RUN uv sync --frozen --no-dev

ENV PATH="/opt/dagster/app/.venv/bin:$PATH"

# @dbt_assets loads transform/evd_transform/target/manifest.json at import
# time, so install dbt packages and build the manifest into the image (parse
# needs no database connection; the credentials only have to be present)
RUN cd transform/evd_transform \
    && dbt deps --profiles-dir .. \
    && PG_USER=dummy PG_PASSWORD=dummy dbt parse --profiles-dir .. --target dev --quiet

# instance config; SQLite storage paths point into the dagster_data volume
RUN mkdir -p "$DAGSTER_HOME"
COPY deploy/dagster.yaml "$DAGSTER_HOME/dagster.yaml"

EXPOSE 3000
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]
