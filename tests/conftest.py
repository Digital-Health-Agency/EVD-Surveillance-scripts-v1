import os

# Importing evd_orchestration submodules executes the top-level package
# __init__.py, which builds Dagster's Definitions() and requires these env
# vars. Unit tests exercise pure functions only and never touch MinIO/Postgres,
# so dummy values are enough to satisfy import-time construction.
os.environ.setdefault("MINIO_ENDPOINT", "http://localhost:9000")
os.environ.setdefault("MINIO_ROOT_USER", "test")
os.environ.setdefault("MINIO_ROOT_PASSWORD", "test")
os.environ.setdefault("PG_HOST", "localhost")
os.environ.setdefault("PG_PORT", "5432")
os.environ.setdefault("PG_DBNAME", "test")
os.environ.setdefault("PG_USER", "test")
os.environ.setdefault("PG_PASSWORD", "test")
