"""Load Uhai Ebola surveillance cases from the Uhai API into MinIO.

The Uhai API returns already-normalized Ebola surveillance records. Each
Dagster partition is passed to the API as a date window and paginated with
limit/offset so the source service is not asked for a large response at once.
"""

from collections.abc import Iterator
from datetime import datetime, timezone
import json
import os
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import dlt

API_HOST_URL = os.getenv("UHAI_API_HOST_URL", "https://chat.nphl.go.ke").rstrip("/")
API_BASE_URL = f"{API_HOST_URL}/api/v1/surveillance/ebola-cases"
PAGE_LIMIT = 100


def _api_key() -> str:
    return dlt.secrets["datasources.uhai.api_key"]


def _date_param(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).date().isoformat()
    if value is None:
        return datetime.now(timezone.utc).date().isoformat()
    return str(value)[:10]


def _fetch_cases(date_from: str, date_to: str, offset: int) -> list[dict[str, Any]]:
    params = urlencode(
        {
            "date_from": date_from,
            "date_to": date_to,
            "limit": PAGE_LIMIT,
            "offset": offset,
        }
    )
    request = Request(
        f"{API_BASE_URL}?{params}",
        headers={
            "Accept": "application/json",
            "x-api-key": _api_key(),
        },
    )

    try:
        with urlopen(request, timeout=60) as response:
            payload = json.load(response)
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Uhai API request failed with HTTP {exc.code}: {detail}") from exc

    cases = payload.get("cases", [])
    if not isinstance(cases, list):
        raise RuntimeError("Uhai API response key 'cases' must be a list")

    return cases


# Fields the original integration mapped explicitly; their normalisation
# (string ids, empty strings for missing values) is unchanged.
_CASE_FIELDS = {
    "system_id", "names", "sex", "age", "date_of_birth", "nationality",
    "identifier_type", "identifier", "suspected", "screening", "confirmed",
    "died", "recovered", "tested", "result", "point_of_entry",
    "reporting_county", "reporting_sub_county", "ward", "facility_fid",
    "community_health_unit_chu", "reporting_date", "reporting_time", "created_at",
}


def _case_record(case: dict[str, Any]) -> dict[str, Any]:
    """Keep every field UHAI sends, normalising the originally mapped ones.

    UHAI's surveillance API added contact, travel, clinical and exposure fields
    on 27 Oct 2026 (phone_number, dependent_phone_number, is_dependent,
    country_of_departure, entry_country, arrival_date, mode_of_transport,
    transport_number, seat_number, temperature, symptoms, risk_factors, the
    exposure questions and source). A fixed field list silently dropped them,
    so any field outside it is now passed through as received; bronze adds a
    column for each automatically.
    """
    extra = {key: value for key, value in case.items() if key not in _CASE_FIELDS}
    return {**extra, **_mapped_fields(case)}


def _mapped_fields(case: dict[str, Any]) -> dict[str, Any]:
    return {
        "system_id": str(case.get("system_id") or ""),
        "names": case.get("names") or "",
        "sex": case.get("sex") or "",
        "age": case.get("age"),
        "date_of_birth": case.get("date_of_birth") or "",
        "nationality": case.get("nationality") or "",
        "identifier_type": case.get("identifier_type") or "",
        "identifier": case.get("identifier") or "",
        "suspected": case.get("suspected") or "",
        "screening": case.get("screening") or "",
        "confirmed": case.get("confirmed") or "",
        "died": case.get("died") or "",
        "recovered": case.get("recovered") or "",
        "tested": case.get("tested") or "",
        "result": case.get("result") or "",
        "point_of_entry": case.get("point_of_entry") or "",
        "reporting_county": case.get("reporting_county") or "",
        "reporting_sub_county": case.get("reporting_sub_county") or "",
        "ward": case.get("ward") or "",
        "facility_fid": case.get("facility_fid") or "",
        "community_health_unit_chu": case.get("community_health_unit_chu") or "",
        "reporting_date": case.get("reporting_date") or "",
        "reporting_time": case.get("reporting_time") or "",
        "created_at": case.get("created_at") or "",
    }


@dlt.source(name="uhai", max_table_nesting=0)
def uhai_source():
    @dlt.resource(
        name="traveler_screenings",
        primary_key="system_id",
        write_disposition="append",
    )
    def traveler_screenings(
        created_at=dlt.sources.incremental(
            "created_at", initial_value="2026-07-03T00:00:00.000Z"
        ),
    ) -> Iterator[list[dict[str, Any]]]:
        date_from = _date_param(created_at.last_value)
        date_to = _date_param(created_at.end_value)
        offset = 0

        while True:
            cases = _fetch_cases(date_from, date_to, offset)
            if not cases:
                break

            yield [_case_record(case) for case in cases]

            if len(cases) < PAGE_LIMIT:
                break
            offset += PAGE_LIMIT

    return traveler_screenings


source = uhai_source()

pipeline = dlt.pipeline(
    pipeline_name="uhai",
    destination="filesystem",
    dataset_name="uhai_raw",
    progress=dlt.progress.tqdm(colour="yellow"),
)
