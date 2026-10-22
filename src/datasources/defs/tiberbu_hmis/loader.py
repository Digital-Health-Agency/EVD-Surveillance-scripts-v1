"""Load Ebola screenings from every Tiberbu HMIS instance into MinIO.

Tiberbu HMIS runs as many separate instances (one per county or group), each
serving many health facilities that screen patients for Ebola at triage. Every
instance exposes its screenings through a Frappe whitelisted method,
`hmis.hmis.clinical.vhf.lake.screenings`, under the same contract as the KRCS
Data Capture Forms source: a half-open [dateStart, dateEnd) window on
`modified` (ISO-8601 UTC, millisecond precision), `page`/`limit` paging and
Frappe's `message` envelope.

The instances are listed in config (`[datasources.tiberbu_hmis.instances.<code>]`
with a `base_url`), each with its own API key and secret in secrets. Each
instance is its own asset, `tiberbu_hmis/screenings_<code>`, so one instance can
be run or backfilled alone; all instances load into the one `screenings` table,
tagged with `instance`. Adding an instance is a config change.

Screenings are pseudonymised at the source (`phiIncluded: false`): names,
identifiers, phone numbers, addresses, staff names and free-text notes are never
sent, and the patient and the screening are keyed hashes (`subjectPseudoId`,
`screeningIdentifier`) that only their HMIS instance can link back. Age, sex and
the screening facility (name, FID, MFL code, county) are kept, so the raw bucket
holds indirectly-identifying data.

The resources are windowed on `modified`, so a screening reappears when it is
edited. Writes are `append`: deduplicate downstream on
(`instance`, `screeningIdentifier`), keeping the latest `modified`.
"""

import re

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# Frappe wraps whitelisted responses in a `message` envelope.
ENDPOINT = "hmis.hmis.clinical.vhf.lake.screenings"
PAGE_SIZE = 200

# earliest data to load; the partition start_date in defs.yaml must match
INITIAL_VALUE = "2026-06-01T00:00:00.000Z"

INSTANCE_CODE = re.compile(r"^[a-z][a-z0-9_]*$")


def _instance_codes() -> list:
    """The configured instance codes. Only their names are read here, so `dg check defs`
    needs no credentials. (The [...instances.<code>] section can sit in both config and
    secrets, so each setting is read by its full path at run time, never from this table.)"""
    configured = dlt.config.get("datasources.tiberbu_hmis.instances") or {}
    for code in configured:
        if not INSTANCE_CODE.match(code):
            raise ValueError(
                f"datasources.tiberbu_hmis.instances: '{code}' must be snake_case (it names an asset)"
            )
    return sorted(configured)


def _client(code: str) -> RESTClient:
    # Read at run time, so a missing setting fails that instance only.
    prefix = f"datasources.tiberbu_hmis.instances.{code}"
    base_url = dlt.config.get(f"{prefix}.base_url")
    key = dlt.secrets.get(f"{prefix}.api_key")
    secret = dlt.secrets.get(f"{prefix}.api_secret")

    missing = [n for n, v in (("base_url", base_url), ("api_key", key), ("api_secret", secret)) if not v]
    if missing:
        raise ValueError(f"{prefix}: missing or empty {', '.join(missing)}")

    return RESTClient(
        base_url=base_url,
        headers={"Authorization": f"token {key}:{secret}"},
        paginator=PageNumberPaginator(base_page=1, total_path="message.pages"),
    )


def _screenings_resource(code: str):
    @dlt.resource(
        name=f"screenings_{code}",
        table_name="screenings",
        primary_key=("instance", "screeningIdentifier"),
        write_disposition="append",
    )
    def screenings(
        modified=dlt.sources.incremental("modified", initial_value=INITIAL_VALUE),
    ):
        params = {"limit": PAGE_SIZE, "dateStart": modified.last_value}
        if modified.end_value:
            params["dateEnd"] = modified.end_value
        for page in _client(code).paginate(ENDPOINT, params=params, data_selector="message.data"):
            yield [{**record, "instance": code} for record in page]

    return screenings


@dlt.source(name="tiberbu_hmis", max_table_nesting=0)
def tiberbu_hmis_source():
    return [_screenings_resource(code) for code in _instance_codes()]


source = tiberbu_hmis_source()

pipeline = dlt.pipeline(
    pipeline_name="tiberbu_hmis",
    destination="filesystem",
    dataset_name="tiberbu_hmis_raw",
    progress=dlt.progress.tqdm(colour="yellow"),
)
