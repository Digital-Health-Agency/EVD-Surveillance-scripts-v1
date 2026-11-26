import duckdb
from dagster import ConfigurableResource


class DuckDBResource(ConfigurableResource):
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        use_ssl = self.s3_endpoint.startswith("https://")
        endpoint = self.s3_endpoint.removeprefix("https://").removeprefix("http://")
        conn = duckdb.connect()
        conn.install_extension("httpfs")
        conn.load_extension("httpfs")
        conn.execute(f"""
            SET s3_endpoint='{endpoint}';
            SET s3_access_key_id='{self.s3_access_key}';
            SET s3_secret_access_key='{self.s3_secret_key}';
            SET s3_use_ssl={str(use_ssl).lower()};
            SET s3_url_style='path';
        """)
        return conn
