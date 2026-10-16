"""Load NPHIIS (FHIR R4) surveillance data into MinIO.

NPHIIS exposes its HAPI FHIR server through the hie-auth `/fhir` proxy,
secured by Keycloak (realm `nphiis`). Authentication is an OAuth2
client-credentials service account. Resources are windowed on
`meta.lastUpdated` (`_lastUpdated=ge..&_lastUpdated=lt..`), so each Dagster
partition run loads one day and history backfills are ordinary backfills.

Direct identifiers (names, contact details, national IDs, street addresses)
are dropped from Patient resources as they stream through. QuestionnaireResponse
resources are loaded whole and may carry identifying answers; see the README
for the follow-up on field-level redaction.
"""

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials
from dlt.sources.helpers.rest_client.paginators import BasePaginator

# earliest data to load; the partition start_date in defs.yaml must match
INITIAL_VALUE = "2026-01-01T00:00:00.000Z"

SCREENING_PROFILE = (
    "http://nphiis.health.go.ke/fhir/StructureDefinition/traveller-screening-response"
)
SCREENED_TAG = "http://nphiis.health.go.ke/fhir/CodeSystem/record-origin|screened"
QUESTIONNAIRE_BASE = "http://nphiis.health.go.ke/Questionnaire/"
VHF_FORMS = ("vhf-case", "vhf-contact-follow-up", "vhf-clinical-outcome")
VHF_LAB_FORM = "vhf-lab-results"

# identifier systems kept on Patient; everything else (national ID, IPRS...) is dropped
KEPT_IDENTIFIER_SYSTEMS = (
    "http://nphiis.health.go.ke/fhir/identifier/screening-id",
    "http://nphiis.health.go.ke/fhir/identifier/client-registry",
)


class FhirBundlePaginator(BasePaginator):
    """Follow the `link` with relation `next` in a FHIR searchset Bundle."""

    def __init__(self):
        super().__init__()
        self._next_url = None

    def update_state(self, response, data=None):
        self._next_url = next(
            (
                link.get("url")
                for link in response.json().get("link", [])
                if link.get("relation") == "next"
            ),
            None,
        )
        self._has_next_page = bool(self._next_url)

    def update_request(self, request):
        request.url = self._next_url


def _client() -> RESTClient:
    """Build a RESTClient at run time so `dg check defs` (import only) never
    needs config or credentials."""
    base_url = dlt.secrets["datasources.nphiis.base_url"]
    token_url = dlt.secrets["datasources.nphiis.token_url"]
    client_id = dlt.secrets["datasources.nphiis.client_id"]
    client_secret = dlt.secrets["datasources.nphiis.client_secret"]

    missing = [
        n
        for n, v in (
            ("base_url", base_url),
            ("token_url", token_url),
            ("client_id", client_id),
            ("client_secret", client_secret),
        )
        if not v
    ]
    if missing:
        raise ValueError(f"datasources.nphiis: missing or empty {', '.join(missing)}")

    return RESTClient(
        base_url=base_url,
        auth=OAuth2ClientCredentials(
            access_token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
        ),
        paginator=FhirBundlePaginator(),
    )


def map_patient(p: dict) -> dict:
    """Drop direct identifiers; keep demographics, area-level address and the
    screening / client-registry identifiers used to join records."""
    identifiers = [
        i for i in p.get("identifier", []) if i.get("system") in KEPT_IDENTIFIER_SYSTEMS
    ]
    addresses = [
        {k: a[k] for k in ("district", "state", "city", "country") if k in a}
        for a in p.get("address", [])
    ]
    return {
        "id": p.get("id"),
        "meta": p.get("meta"),
        "identifier": identifiers,
        "gender": p.get("gender"),
        "birthDate": p.get("birthDate"),
        "address": addresses,
    }


def _search(path: str, params: dict, last_updated, mapper=None):
    """Yield pages of resources for one `_lastUpdated` window."""
    window = [f"ge{last_updated.last_value}"]
    if last_updated.end_value:
        window.append(f"lt{last_updated.end_value}")
    query = {"_count": 100, "_sort": "_lastUpdated", **params, "_lastUpdated": window}
    for page in _client().paginate(path, params=query, data_selector="entry"):
        resources = [e["resource"] for e in page if "resource" in e]
        yield [mapper(r) for r in resources] if mapper else resources


def _questionnaires(*names: str) -> str:
    return ",".join(QUESTIONNAIRE_BASE + n for n in names)


@dlt.source(name="nphiis", max_table_nesting=0)
def nphiis_source():
    def cursor():
        return dlt.sources.incremental("meta.lastUpdated", initial_value=INITIAL_VALUE)

    @dlt.resource(
        name="traveller_screenings", primary_key="id", write_disposition="append"
    )
    def traveller_screenings(last_updated=cursor()):
        yield from _search(
            "QuestionnaireResponse", {"_profile": SCREENING_PROFILE}, last_updated
        )

    @dlt.resource(name="screened_patients", primary_key="id", write_disposition="append")
    def screened_patients(last_updated=cursor()):
        yield from _search("Patient", {"_tag": SCREENED_TAG}, last_updated, map_patient)

    @dlt.resource(
        name="vhf_questionnaire_responses",
        primary_key="id",
        write_disposition="append",
    )
    def vhf_questionnaire_responses(last_updated=cursor()):
        yield from _search(
            "QuestionnaireResponse",
            {"questionnaire": _questionnaires(*VHF_FORMS)},
            last_updated,
        )

    @dlt.resource(name="vhf_lab_results", primary_key="id", write_disposition="append")
    def vhf_lab_results(last_updated=cursor()):
        yield from _search(
            "QuestionnaireResponse",
            {"questionnaire": _questionnaires(VHF_LAB_FORM)},
            last_updated,
        )

    @dlt.resource(name="locations", primary_key="id", write_disposition="append")
    def locations(last_updated=cursor()):
        yield from _search("Location", {}, last_updated)

    return (
        traveller_screenings,
        screened_patients,
        vhf_questionnaire_responses,
        vhf_lab_results,
        locations,
    )


source = nphiis_source()

pipeline = dlt.pipeline(
    pipeline_name="nphiis",
    destination="filesystem",
    dataset_name="nphiis_raw",
    progress=dlt.progress.tqdm(colour="yellow"),
)
