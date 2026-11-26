"""Bootstrap the bronze/silver/gold schemas. Run via `make migrate`.

There is no migration tool/ORM for bronze — its table shape is inferred and
evolved by the Dagster asset at ingest time (see assets/bronze/ingest.py).
This script only ensures the three schemas exist before anything writes to
them; silver/gold tables are created by `dbt build`.
"""

import os
from pathlib import Path

import psycopg2

INIT_SQL = Path(__file__).parents[3] / "infra" / "postgres" / "init.sql"


def main() -> None:
    conn = psycopg2.connect(
        host=os.environ["PG_HOST"],
        port=int(os.environ["PG_PORT"]),
        dbname=os.environ["PG_DBNAME"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(INIT_SQL.read_text())
        conn.commit()
    finally:
        conn.close()
    print(f"Applied {INIT_SQL}")


if __name__ == "__main__":
    main()
