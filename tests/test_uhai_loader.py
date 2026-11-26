"""Tests for the UHAI loader's record shaping (no network)."""
from datasources.defs.uhai.loader import _CASE_FIELDS, _case_record

NEW_API_FIELDS = {
    "phone_number": "+254712345678",
    "is_dependent": "No",
    "dependent_phone_number": "",
    "country_of_departure": "uganda",
    "arrival_date": "2026-10-27",
    "mode_of_transport": "AIR",
    "transport_number": "KQ413",
    "seat_number": "14C",
    "temperature": 37.8,
    "funeral_body_prep": "No",
    "source": "whatsapp",
}


def _case(**extra):
    return {"system_id": 1234, "names": "Test Traveller", "sex": "Female", "age": 34, **extra}


def test_new_api_fields_are_kept_as_received():
    record = _case_record(_case(**NEW_API_FIELDS))
    for key, value in NEW_API_FIELDS.items():
        assert record[key] == value


def test_mapped_fields_keep_their_normalisation():
    record = _case_record(_case(identifier=None))
    assert record["system_id"] == "1234"
    assert record["identifier"] == ""
    assert record["point_of_entry"] == ""


def test_every_originally_mapped_field_is_present():
    record = _case_record(_case())
    assert _CASE_FIELDS <= set(record)


def test_a_new_field_cannot_override_a_mapped_one():
    record = _case_record({"system_id": 7, "names": None})
    assert record["system_id"] == "7"
    assert record["names"] == ""
