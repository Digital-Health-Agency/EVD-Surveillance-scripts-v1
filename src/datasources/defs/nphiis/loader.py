"""Load NPHIIS VHF line lists (cases and contacts) into MinIO.

NPHIIS exposes two plain REST report endpoints through hie-auth
(`/reports/vhf/line-list?recordType=Case` and `...?recordType=Contact`),
secured by either an OAuth2 client-credentials bearer token (Keycloak realm
`nphiis`) or an `X-Api-Key` header. The endpoints take a date window
(`from=YYYY-MM-DD&to=YYYY-MM-DD`, `to` assumed inclusive), so each Dagster
partition run loads one day and history backfills are ordinary backfills.

The line list carries no known timestamp usable as a dlt cursor, so each
record is stamped with `window_start` (the run's window start, ISO-8601 Z)
and the incremental cursor is declared on that field; the API does the actual
date filtering. Pages are walked with `limit`/`offset` until an empty page
comes back (the response exposes no total; set `total_path` once it does).
"""

from datetime import datetime, timedelta

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import APIKeyAuth, OAuth2ClientCredentials
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator

LINE_LIST_PATH = "reports/vhf/line-list"
PAGE_SIZE = 100

# earliest data to load; the partition start_date in defs.yaml must match
INITIAL_VALUE = "2026-01-01T00:00:00.000Z"


def _auth():
    """`X-Api-Key` when an api_key secret is set, otherwise Keycloak
    client-credentials (bearer token)."""
    api_key = dlt.secrets.get("datasources.nphiis.api_key")
    if api_key:
        return APIKeyAuth(name="X-Api-Key", api_key=api_key, location="header")

    token_url = dlt.secrets.get("datasources.nphiis.token_url")
    client_id = dlt.secrets.get("datasources.nphiis.client_id")
    client_secret = dlt.secrets.get("datasources.nphiis.client_secret")
    missing = [
        n
        for n, v in (
            ("token_url", token_url),
            ("client_id", client_id),
            ("client_secret", client_secret),
        )
        if not v
    ]
    if missing:
        raise ValueError(
            "datasources.nphiis: set api_key, or all of "
            f"token_url/client_id/client_secret (missing {', '.join(missing)})"
        )
    return OAuth2ClientCredentials(
        access_token_url=token_url, client_id=client_id, client_secret=client_secret
    )


def _client() -> RESTClient:
    """Build a RESTClient at run time so `dg check defs` (import only) never
    needs config or credentials."""
    base_url = dlt.secrets.get("datasources.nphiis.base_url")
    if not base_url:
        raise ValueError("datasources.nphiis: missing or empty base_url")
    return RESTClient(
        base_url=base_url,
        auth=_auth(),
        paginator=OffsetPaginator(limit=PAGE_SIZE, offset=0, total_path=None),
    )


def _date(iso_z: str) -> datetime:
    return datetime.fromisoformat(iso_z.replace("Z", "+00:00"))


def _line_list(record_type: str, window_start):
    """Yield pages of line-list records for one window, each stamped with
    `window_start` so the dlt cursor can be evaluated."""
    params = {"recordType": record_type, "from": _date(window_start.last_value).date()}
    if window_start.end_value:
        # window end is exclusive, the API's `to` is an inclusive date
        params["to"] = (_date(window_start.end_value) - timedelta(days=1)).date()
    for page in _client().paginate(LINE_LIST_PATH, params=params):
        yield [{**record, "window_start": window_start.last_value} for record in page]


@dlt.source(name="nphiis", max_table_nesting=0)
def nphiis_source():
    def cursor():
        return dlt.sources.incremental("window_start", initial_value=INITIAL_VALUE)

    @dlt.resource(name="cases", write_disposition="append")
    def cases(window_start=cursor()):
        yield from _line_list("Case", window_start)

    @dlt.resource(name="contacts", write_disposition="append")
    def contacts(window_start=cursor()):
        yield from _line_list("Contact", window_start)

    return cases, contacts


source = nphiis_source()

pipeline = dlt.pipeline(
    pipeline_name="nphiis",
    destination="filesystem",
    dataset_name="nphiis_raw",
    progress=dlt.progress.tqdm(colour="yellow"),
)
