"""Debezium connector configuration builder."""

from __future__ import annotations

from dagster_project.models.dataset_config import DatasetDefinition


def build_connector_name(dataset: DatasetDefinition) -> str:
    """Generate a standardized connector name for a dataset."""
    db = dataset.source.mongodb.database
    col = dataset.source.mongodb.collection
    return f"mongo-{db}-{col}"


def build_topic_prefix(dataset: DatasetDefinition) -> str:
    """Generate the Debezium topic prefix.

    Debezium generates topics as: {prefix}.{database}.{collection}
    We use 'cdc' as prefix so topics are: cdc.app_db.users
    """
    return "cdc"


def build_debezium_config(
    dataset: DatasetDefinition,
    mongodb_uri: str,
) -> dict[str, str]:
    """Build the full Debezium MongoDB connector configuration.

    This config uses ExtractNewDocumentState to flatten the CDC envelope
    into a plain document, making it directly consumable by ClickHouse.
    """
    db = dataset.source.mongodb.database
    col = dataset.source.mongodb.collection
    topic_prefix = build_topic_prefix(dataset)

    return {
        "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
        "mongodb.connection.string": mongodb_uri,
        "topic.prefix": topic_prefix,
        "collection.include.list": f"{db}.{col}",
        # Flatten nested Debezium envelope → plain document
        "transforms": "unwrap",
        "transforms.unwrap.type": (
            "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState"
        ),
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "transforms.unwrap.add.fields": "op,source.ts_ms",
        # Snapshot: initial = dump existing + then stream
        "snapshot.mode": "initial",
        # JSON without schemas for simplicity
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        # Heartbeat
        "heartbeat.interval.ms": "10000",
    }
