"""
Schema evolution integration test.
Requires a running Schema Registry at SCHEMA_REGISTRY_URL (default: http://localhost:8081).

Run:
    pytest schemas/test_evolution.py -v
"""

import json
import os
import uuid
import copy

import pytest
import requests

REGISTRY_URL: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# Use a unique test subject so tests don't interfere with production subjects.
TEST_SUBJECT = f"test-evolution-{uuid.uuid4().hex[:8]}-value"

HEADERS = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

# ---------------------------------------------------------------------------
# Minimal TransactionEvent v1 (no network_type field)
# ---------------------------------------------------------------------------

TRANSACTION_EVENT_V1: dict = {
    "namespace": "com.fraudplatform.events",
    "type": "record",
    "name": "TransactionEvent",
    "fields": [
        {"name": "transaction_id", "type": "string"},
        {"name": "card_id",        "type": "string"},
        {"name": "user_id",        "type": "string"},
        {"name": "amount",         "type": "double"},
        {"name": "merchant_id",    "type": "string"},
        {"name": "merchant_name",  "type": "string"},
        {"name": "merchant_cat",   "type": "string"},
        {
            "name": "location",
            "type": {
                "type": "record",
                "name": "GeoPoint",
                "namespace": "com.fraudplatform.events",
                "fields": [
                    {"name": "lat", "type": "double"},
                    {"name": "lon", "type": "double"},
                ],
            },
        },
        {"name": "country",   "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "is_online", "type": "boolean"},
        {"name": "device_id", "type": ["null", "string"], "default": None},
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _register(subject: str, schema: dict) -> int:
    """Register a schema and return the schema ID."""
    url = f"{REGISTRY_URL}/subjects/{subject}/versions"
    resp = requests.post(url, json={"schema": json.dumps(schema)}, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()["id"]


def _check_compat(subject: str, schema: dict) -> bool:
    url = f"{REGISTRY_URL}/compatibility/subjects/{subject}/versions/latest"
    resp = requests.post(url, json={"schema": json.dumps(schema)}, headers=HEADERS)
    if resp.status_code == 404:
        return True  # No existing version — trivially compatible.
    resp.raise_for_status()
    return resp.json().get("is_compatible", False)


def _get_latest(subject: str) -> dict:
    url = f"{REGISTRY_URL}/subjects/{subject}/versions/latest"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()


def _delete_subject(subject: str) -> None:
    """Soft-delete and hard-delete the test subject for cleanup."""
    for mode in ("", "?permanent=true"):
        requests.delete(f"{REGISTRY_URL}/subjects/{subject}{mode}", headers=HEADERS)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def cleanup_test_subject():
    """Delete the test subject before and after each test for isolation."""
    _delete_subject(TEST_SUBJECT)
    yield
    _delete_subject(TEST_SUBJECT)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSchemaEvolution:

    def test_register_v1(self):
        """Register TransactionEvent v1 and verify it gets a positive schema ID."""
        schema_id = _register(TEST_SUBJECT, TRANSACTION_EVENT_V1)
        assert isinstance(schema_id, int)
        assert schema_id > 0, "Schema Registry must return a positive integer schema ID"

        latest = _get_latest(TEST_SUBJECT)
        assert latest["version"] == 1

    def test_new_optional_field_is_backward_compatible(self):
        """
        Adding a new nullable field with a default is a BACKWARD-compatible change.
        A v1 consumer must be able to read v2 messages (it simply ignores the new field).
        """
        _register(TEST_SUBJECT, TRANSACTION_EVENT_V1)

        v2 = copy.deepcopy(TRANSACTION_EVENT_V1)
        v2["fields"].append(
            {
                "name": "network_type",
                "type": ["null", "string"],
                "default": None,
                "doc": "e.g. WiFi, 5G, LTE — null for POS terminals",
            }
        )

        is_compat = _check_compat(TEST_SUBJECT, v2)
        assert is_compat, (
            "Adding a nullable field with a default must be BACKWARD compatible "
            "so that existing v1 consumers can still read v2 messages."
        )

    def test_register_v2_gets_new_schema_id(self):
        """Registering a changed schema must produce a schema ID different from v1."""
        id_v1 = _register(TEST_SUBJECT, TRANSACTION_EVENT_V1)

        v2 = copy.deepcopy(TRANSACTION_EVENT_V1)
        v2["fields"].append(
            {"name": "network_type", "type": ["null", "string"], "default": None}
        )

        id_v2 = _register(TEST_SUBJECT, v2)

        assert id_v2 != id_v1, (
            "A new schema version must receive a distinct schema ID from v1."
        )

        latest = _get_latest(TEST_SUBJECT)
        assert latest["version"] == 2, "Latest version must be 2 after registering v2."
        assert latest["id"] == id_v2

    def test_removing_a_field_is_not_backward_compatible(self):
        """
        Adding a required field (no default) IS backward incompatible:
        the new schema cannot read old messages that lack the field.
        
        NOTE: Under Avro BACKWARD rules, *removing* a field is actually fine
        (old data's extra field is ignored by the new reader). What breaks 
        BACKWARD compatibility is *adding* a field with no default.
        """
        _register(TEST_SUBJECT, TRANSACTION_EVENT_V1)

        broken = copy.deepcopy(TRANSACTION_EVENT_V1)
        # Add a required field with no default — old messages won't have it,
        # so the new schema cannot deserialize them.
        broken["fields"].append(
            {
                "name": "required_new_field",
                "type": "string",
                # No "default" — this is what breaks BACKWARD compatibility.
                "doc": "A required field old producers never wrote",
            }
        )

        is_compat = _check_compat(TEST_SUBJECT, broken)
        assert not is_compat, (
            "Adding a required field with no default must be BACKWARD INCOMPATIBLE: "
            "old messages lack this field and the new reader has no default to fall back on."
        )

    def test_v1_consumer_can_deserialize_v2_message(self):
        """
        Functional round-trip: serialize with v2 schema, deserialize with v1 reader schema.
        The new field 'network_type' must be silently ignored by the v1 reader.

        NOTE: This test exercises the Avro projection / reader-schema feature directly
        using the fastavro library (not the Schema Registry wire format) so it can run
        without a running Kafka cluster.
        """
        pytest.importorskip("fastavro", reason="fastavro required for this round-trip test")
        import io
        import fastavro

        v2_schema = copy.deepcopy(TRANSACTION_EVENT_V1)
        v2_schema["fields"].append(
            {"name": "network_type", "type": ["null", "string"], "default": None}
        )

        v2_record = {
            "transaction_id": str(uuid.uuid4()),
            "card_id": "CARD_0000001",
            "user_id": "USER_000001",
            "amount": 49.99,
            "merchant_id": "MER_001",
            "merchant_name": "Corner Store",
            "merchant_cat": "5411",
            "location": {"lat": 40.71, "lon": -74.01},
            "country": "US",
            "timestamp": 1_700_000_000_000,
            "is_online": False,
            "device_id": None,
            "network_type": "5G",  # new v2 field
        }

        # Serialize with v2 writer schema.
        buf = io.BytesIO()
        parsed_writer = fastavro.parse_schema(v2_schema)
        fastavro.schemaless_writer(buf, parsed_writer, v2_record)
        buf.seek(0)

        # Deserialize with v1 reader schema — 'network_type' must be silently dropped.
        parsed_reader = fastavro.parse_schema(TRANSACTION_EVENT_V1)
        v1_record = fastavro.schemaless_reader(buf, parsed_writer, parsed_reader)

        assert "network_type" not in v1_record, (
            "A v1 reader schema must NOT expose the 'network_type' field added in v2."
        )
        assert v1_record["amount"] == pytest.approx(49.99)
        assert v1_record["card_id"] == "CARD_0000001"