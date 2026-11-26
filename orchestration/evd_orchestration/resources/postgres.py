from contextlib import contextmanager

import psycopg2
from dagster import ConfigurableResource


class PostgresResource(ConfigurableResource):
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @contextmanager
    def get_connection(self):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
