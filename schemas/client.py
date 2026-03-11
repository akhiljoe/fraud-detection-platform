"""
Schema client module.
Provides cached Avro serializers and deserializers backed by Schema Registry.

Usage:
    from schemas.client import SchemaClient
    client = SchemaClient("http://localhost:8081")
    raw_bytes = client.serialize("TransactionEvent", event_dict, "transactions.raw")
    event     = client.deserialize("TransactionEvent", raw_bytes, "transactions.raw")
"""

import json
import logging
import os
from pathlib import Path
from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

log = logging.getLogger(__name__)

SCHEMAS_DIR = Path(__file__).parent

# Maps friendly name -> (avsc filename stem, subject name)
_SCHEMA_META: dict[str, tuple[str, str]] = {
    "TransactionEvent":    ("transaction_event",    "transactions.raw-value"),
    "EnrichedTransaction": ("enriched_transaction", "transactions.enriched-value"),
    "FraudAlert":          ("fraud_alert",          "fraud.alerts-value"),
}

# Module-level caches — keyed by (schema_name, registry_url)
_serializer_cache:   dict[tuple[str, str], AvroSerializer]   = {}
_deserializer_cache: dict[tuple[str, str], AvroDeserializer] = {}
_registry_client_cache: dict[str, SchemaRegistryClient] = {}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_registry_client(registry_url: str) -> SchemaRegistryClient:
    if registry_url not in _registry_client_cache:
        _registry_client_cache[registry_url] = SchemaRegistryClient({"url": registry_url})
    return _registry_client_cache[registry_url]


def _load_schema_str(schema_name: str) -> str:
    file_stem, _ = _SCHEMA_META[schema_name]
    avsc_path = SCHEMAS_DIR / f"{file_stem}.avsc"
    return avsc_path.read_text()


# ---------------------------------------------------------------------------
# Public factory functions
# ---------------------------------------------------------------------------

def get_avro_serializer(schema_name: str, registry_url: str) -> AvroSerializer:
    """
    Return a cached AvroSerializer for the given schema.

    Args:
        schema_name: One of 'TransactionEvent', 'EnrichedTransaction', 'FraudAlert'.
        registry_url: Schema Registry base URL.

    Returns:
        AvroSerializer instance (cached after first call).
    """
    cache_key = (schema_name, registry_url)
    if cache_key not in _serializer_cache:
        if schema_name not in _SCHEMA_META:
            raise ValueError(f"Unknown schema name '{schema_name}'. Valid: {list(_SCHEMA_META)}")

        registry_client = _get_registry_client(registry_url)
        schema_str = _load_schema_str(schema_name)

        _serializer_cache[cache_key] = AvroSerializer(
            schema_registry_client=registry_client,
            schema_str=schema_str,
            to_dict=lambda obj, ctx: obj,  # dicts pass through unchanged
        )
        log.debug("Created AvroSerializer for '%s'", schema_name)

    return _serializer_cache[cache_key]


def get_avro_deserializer(schema_name: str, registry_url: str) -> AvroDeserializer:
    """
    Return a cached AvroDeserializer for the given schema.

    Args:
        schema_name: One of 'TransactionEvent', 'EnrichedTransaction', 'FraudAlert'.
        registry_url: Schema Registry base URL.

    Returns:
        AvroDeserializer instance (cached after first call).
    """
    cache_key = (schema_name, registry_url)
    if cache_key not in _deserializer_cache:
        if schema_name not in _SCHEMA_META:
            raise ValueError(f"Unknown schema name '{schema_name}'. Valid: {list(_SCHEMA_META)}")

        registry_client = _get_registry_client(registry_url)
        schema_str = _load_schema_str(schema_name)

        _deserializer_cache[cache_key] = AvroDeserializer(
            schema_registry_client=registry_client,
            schema_str=schema_str,
            from_dict=lambda obj, ctx: obj,  # return plain dicts
        )
        log.debug("Created AvroDeserializer for '%s'", schema_name)

    return _deserializer_cache[cache_key]


# ---------------------------------------------------------------------------
# High-level SchemaClient class
# ---------------------------------------------------------------------------

class SchemaClient:
    """
    Convenience wrapper around the Avro serializer/deserializer factories.

    Example:
        client = SchemaClient("http://localhost:8081")
        raw = client.serialize("TransactionEvent", event_dict, "transactions.raw")
        obj = client.deserialize("TransactionEvent", raw, "transactions.raw")
    """

    def __init__(self, registry_url: str | None = None) -> None:
        self.registry_url: str = (
            registry_url
            or os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
        )
        log.info("SchemaClient initialised with registry: %s", self.registry_url)

    def serialize(self, schema_name: str, data: dict[str, Any], topic: str) -> bytes:
        """
        Serialise a dict to Avro bytes using the wire format expected by Schema Registry.

        Args:
            schema_name: Logical schema name ('TransactionEvent', etc.).
            data:        Plain Python dict matching the schema.
            topic:       Kafka topic name (used to build the SerializationContext).

        Returns:
            Wire-format Avro bytes (magic byte + schema ID + payload).
        """
        serializer = get_avro_serializer(schema_name, self.registry_url)
        ctx = SerializationContext(topic, MessageField.VALUE)
        return serializer(data, ctx)

    def deserialize(self, schema_name: str, data: bytes, topic: str) -> dict[str, Any]:
        """
        Deserialise Avro wire-format bytes back to a plain Python dict.

        Args:
            schema_name: Logical schema name ('TransactionEvent', etc.).
            data:        Raw bytes from a Kafka message value.
            topic:       Kafka topic name (used to build the SerializationContext).

        Returns:
            Plain Python dict.
        """
        deserializer = get_avro_deserializer(schema_name, self.registry_url)
        ctx = SerializationContext(topic, MessageField.VALUE)
        return deserializer(data, ctx)
