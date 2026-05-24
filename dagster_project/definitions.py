"""Data Control Plane — Dagster Definitions Entry Point.

This module wires everything together:
  - Loads dataset YAML configs and generates onboarding assets via the factory
  - Registers all resources (Kafka, ClickHouse, Debezium, MongoDB)
  - Registers the YAML sensor for auto-detecting new datasets
"""

from __future__ import annotations

import os

import dagster as dg

from dagster_project.assets.dataset_factory import build_all_dataset_assets
from dagster_project.resources.clickhouse_resource import ClickHouseResource
from dagster_project.resources.debezium_resource import DebeziumResource
from dagster_project.resources.kafka_resource import KafkaAdminResource
from dagster_project.resources.mongodb_resource import MongoDBResource
from dagster_project.sensors.yaml_sensor import dataset_yaml_sensor


def build_definitions() -> dg.Definitions:
    """Build the complete Dagster Definitions object."""

    # ── Resources ──
    resources = {
        "kafka_admin": KafkaAdminResource(
            bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
            ),
        ),
        "clickhouse": ClickHouseResource(
            host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
            username=os.environ.get("CLICKHOUSE_USER", "default"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        ),
        "debezium": DebeziumResource(
            connect_url=os.environ.get(
                "KAFKA_CONNECT_URL", "http://kafka-connect:8083"
            ),
        ),
        "mongodb": MongoDBResource(
            uri=os.environ.get(
                "MONGODB_URI",
                "mongodb://mongodb:27017/?replicaSet=rs0&directConnection=true",
            ),
            default_database=os.environ.get("MONGODB_DATABASE", "app_db"),
        ),
    }

    # ── Assets (generated from YAML) ──
    datasets_dir = os.environ.get("DATASETS_DIR", None)
    all_assets = build_all_dataset_assets(datasets_dir)

    # ── Sensors ──
    sensors = [dataset_yaml_sensor]

    return dg.Definitions(
        assets=all_assets,
        resources=resources,
        sensors=sensors,
    )


defs = build_definitions()
