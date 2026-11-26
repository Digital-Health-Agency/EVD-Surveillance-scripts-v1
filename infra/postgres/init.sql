-- Medallion schemas. Bronze table shapes are inferred/evolved at ingest time
-- (see orchestration/evd_orchestration/assets/bronze/ingest.py) — nothing to
-- migrate here beyond the schemas themselves. Silver/gold tables are created
-- by `dbt build` (transform/evd_transform).

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
