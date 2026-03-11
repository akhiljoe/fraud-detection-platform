"""
Idempotent Schema Registry setup script.
Registers all .avsc schemas in this directory. Safe to re-run at any time.

Usage:
    SCHEMA_REGISTRY_URL=http://localhost:8081 python schemas/schema_registry_setup.py
"""

import json
import logging
import os
import sys
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCHEMA_REGISTRY_URL: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# Maps .avsc filename stem -> Schema Registry subject name.
# Order matters: register dependencies (GeoPoint is embedded) first.
SCHEMA_SUBJECTS: dict[str, str] = {
    "transaction_event":    "transactions.raw-value",
    "enriched_transaction": "transactions.enriched-value",
    "fraud_alert":          "fraud.alerts-value",
}

SCHEMAS_DIR = Path(__file__).parent


# ---------------------------------------------------------------------------
# HTTP session with retries
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
    )
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"Content-Type": "application/vnd.schemaregistry.v1+json"})
    return session


# ---------------------------------------------------------------------------
# Schema Registry helpers
# ---------------------------------------------------------------------------

def _get_latest_schema(session: requests.Session, subject: str) -> dict | None:
    """Return the latest registered schema dict, or None if the subject doesn't exist."""
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
    response = session.get(url)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def _schemas_are_equal(registered_schema_str: str, candidate_schema: dict) -> bool:
    """Compare schemas by normalising both to sorted JSON."""
    try:
        registered = json.loads(registered_schema_str)
    except json.JSONDecodeError:
        return False
    return json.dumps(registered, sort_keys=True) == json.dumps(candidate_schema, sort_keys=True)


def _check_compatibility(
    session: requests.Session, subject: str, schema: dict
) -> bool:
    """
    POST to the compatibility endpoint.
    Returns True if BACKWARD compatible, False otherwise.
    """
    url = f"{SCHEMA_REGISTRY_URL}/compatibility/subjects/{subject}/versions/latest"
    payload = {"schema": json.dumps(schema)}
    response = session.post(url, json=payload)
    if response.status_code == 404:
        # Subject doesn't exist yet — trivially compatible.
        return True
    response.raise_for_status()
    result = response.json()
    return result.get("is_compatible", False)


def _register_schema(session: requests.Session, subject: str, schema: dict) -> int:
    """Register the schema and return the assigned schema ID."""
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
    payload = {"schema": json.dumps(schema)}
    response = session.post(url, json=payload)
    response.raise_for_status()
    schema_id: int = response.json()["id"]
    return schema_id


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def setup_schemas() -> None:
    session = _build_session()

    log.info("Connecting to Schema Registry at %s", SCHEMA_REGISTRY_URL)

    # Verify connectivity.
    try:
        session.get(f"{SCHEMA_REGISTRY_URL}/subjects").raise_for_status()
    except requests.RequestException as exc:
        log.error("Cannot reach Schema Registry: %s", exc)
        sys.exit(1)

    for file_stem, subject in SCHEMA_SUBJECTS.items():
        avsc_path = SCHEMAS_DIR / f"{file_stem}.avsc"

        if not avsc_path.exists():
            log.error("Schema file not found: %s", avsc_path)
            sys.exit(1)

        schema = json.loads(avsc_path.read_text())
        log.info("Processing subject '%s' from %s", subject, avsc_path.name)

        # --- Check if subject already exists ---
        latest = _get_latest_schema(session, subject)

        if latest is not None:
            registered_schema_str: str = latest["schema"]

            if _schemas_are_equal(registered_schema_str, schema):
                log.info(
                    "  [SKIP] Subject '%s' already registered and unchanged (id=%s, version=%s).",
                    subject, latest["id"], latest["version"],
                )
                continue

            # Schema has changed — verify BACKWARD compatibility before proceeding.
            log.info("  Schema has changed. Checking BACKWARD compatibility...")
            is_compatible = _check_compatibility(session, subject, schema)

            if not is_compatible:
                raise RuntimeError(
                    f"Schema change for subject '{subject}' is NOT BACKWARD compatible. "
                    "Aborting to protect consumers. Fix the schema and retry."
                )

            log.info("  Compatibility check passed.")

        # --- Register (new subject or compatible update) ---
        schema_id = _register_schema(session, subject, schema)
        log.info("  [REGISTERED] Subject '%s' -> schema id=%d", subject, schema_id)

    log.info("Schema setup complete.")


if __name__ == "__main__":
    setup_schemas()