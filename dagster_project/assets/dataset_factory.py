"""Dataset Asset Factory — generates Dagster assets from YAML definitions.

For each dataset YAML, this factory produces 4 ordered assets:
  1. kafka_topic_{name}       → Create the Kafka topic
  2. debezium_connector_{name} → Register the Debezium CDC connector
  3. clickhouse_tables_{name}  → Create Kafka queue + target table + MV in ClickHouse
  4. historical_load_{name}    → Bulk load existing MongoDB data into ClickHouse

After onboarding completes, new inserts/updates in MongoDB flow automatically
through Debezium → Kafka → ClickHouse materialized view.
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
import yaml

from dagster_project.models.dataset_config import DatasetDefinition, DatasetYAML
from dagster_project.resources.clickhouse_resource import ClickHouseResource
from dagster_project.resources.debezium_resource import DebeziumResource
from dagster_project.resources.kafka_resource import KafkaAdminResource
from dagster_project.resources.mongodb_resource import MongoDBResource
from dagster_project.utils.clickhouse_ddl import (
    generate_create_database,
    generate_kafka_queue_ddl,
    generate_materialized_view_ddl,
    generate_target_table_ddl,
)
from dagster_project.utils.debezium_config import build_connector_name, build_debezium_config


def load_dataset_configs(datasets_dir: str | None = None) -> list[DatasetDefinition]:
    """Load and validate all dataset YAML files from the datasets directory."""
    if datasets_dir is None:
        datasets_dir = os.environ.get(
            "DATASETS_DIR",
            str(Path(__file__).parent.parent.parent / "datasets"),
        )

    datasets_path = Path(datasets_dir)
    if not datasets_path.exists():
        return []

    configs = []
    for yaml_file in sorted(datasets_path.glob("*.yaml")):
        with open(yaml_file) as f:
            raw = yaml.safe_load(f)
        if raw and "dataset" in raw:
            parsed = DatasetYAML.model_validate(raw)
            configs.append(parsed.dataset)

    return configs


def build_dataset_assets(dataset: DatasetDefinition) -> list[dg.AssetsDefinition]:
    """Build the 4 onboarding assets for a single dataset definition."""
    name = dataset.name

    # ──────────────────────────────────────────────
    # Asset 1: Create Kafka Topic
    # ──────────────────────────────────────────────
    @dg.asset(
        name=f"kafka_topic_{name}",
        group_name=f"onboarding_{name}",
        metadata={
            "dataset": name,
            "topic": dataset.kafka.topic_name,
            "partitions": dataset.kafka.partitions,
        },
        description=f"Create Kafka topic for dataset '{name}'",
    )
    def kafka_topic_asset(
        context,
        kafka_admin: KafkaAdminResource,
    ) -> dg.MaterializeResult:
        topic = dataset.kafka.topic_name
        partitions = dataset.kafka.partitions
        replication = dataset.kafka.replication_factor

        context.log.info(f"Creating Kafka topic: {topic} (partitions={partitions})")

        created = kafka_admin.create_topic(
            topic_name=topic,
            num_partitions=partitions,
            replication_factor=replication,
        )

        if created:
            context.log.info(f"✅ Topic '{topic}' created successfully")
        else:
            context.log.info(f"ℹ️ Topic '{topic}' already exists")

        return dg.MaterializeResult(
            metadata={
                "topic": topic,
                "created": created,
                "partitions": partitions,
            }
        )

    # ──────────────────────────────────────────────
    # Asset 2: Register Debezium Connector
    # ──────────────────────────────────────────────
    @dg.asset(
        name=f"debezium_connector_{name}",
        group_name=f"onboarding_{name}",
        deps=[dg.AssetKey(f"kafka_topic_{name}")],
        metadata={"dataset": name},
        description=f"Register Debezium MongoDB connector for dataset '{name}'",
    )
    def debezium_connector_asset(
        context,
        debezium: DebeziumResource,
    ) -> dg.MaterializeResult:
        connector_name = build_connector_name(dataset)
        mongodb_uri = os.environ.get(
            "MONGODB_URI",
            "mongodb://mongodb:27017/?replicaSet=rs0&directConnection=true",
        )

        context.log.info(f"Registering Debezium connector: {connector_name}")

        if debezium.connector_exists(connector_name):
            context.log.info(f"ℹ️ Connector '{connector_name}' already exists")
            return dg.MaterializeResult(
                metadata={"connector": connector_name, "created": False}
            )

        config = build_debezium_config(dataset, mongodb_uri)
        debezium.create_connector(connector_name, config)
        context.log.info(f"⏳ Waiting for connector to reach RUNNING state...")

        is_running = debezium.wait_for_connector_running(
            connector_name, timeout_seconds=90
        )

        if is_running:
            context.log.info(f"✅ Connector '{connector_name}' is RUNNING")
        else:
            context.log.warning(
                f"⚠️ Connector '{connector_name}' not yet RUNNING — "
                "it may still be performing the initial snapshot"
            )

        return dg.MaterializeResult(
            metadata={
                "connector": connector_name,
                "created": True,
                "running": is_running,
            }
        )

    # ──────────────────────────────────────────────
    # Asset 3: Create ClickHouse Tables
    # ──────────────────────────────────────────────
    @dg.asset(
        name=f"clickhouse_tables_{name}",
        group_name=f"onboarding_{name}",
        deps=[dg.AssetKey(f"debezium_connector_{name}")],
        metadata={"dataset": name},
        description=f"Create ClickHouse tables (Kafka queue + target + MV) for dataset '{name}'",
    )
    def clickhouse_tables_asset(
        context,
        clickhouse: ClickHouseResource,
    ) -> dg.MaterializeResult:
        ch = dataset.clickhouse
        kafka_brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

        # 1. Create database
        db_ddl = generate_create_database(ch.database)
        context.log.info(f"Creating database: {ch.database}")
        clickhouse.execute(db_ddl)

        # 2. Create target table (ReplacingMergeTree)
        target_ddl = generate_target_table_ddl(dataset)
        context.log.info(f"Creating target table: {ch.database}.{ch.table_name}")
        context.log.debug(f"DDL:\n{target_ddl}")
        clickhouse.execute(target_ddl)

        # 3. Create Kafka queue table
        queue_ddl = generate_kafka_queue_ddl(dataset, kafka_brokers=kafka_brokers)
        queue_name = f"kafka_queue_{name}"
        context.log.info(f"Creating Kafka queue table: {ch.database}.{queue_name}")
        context.log.debug(f"DDL:\n{queue_ddl}")
        clickhouse.execute(queue_ddl)

        # 4. Create Materialized View
        mv_ddl = generate_materialized_view_ddl(dataset)
        mv_name = f"mv_{name}"
        context.log.info(f"Creating materialized view: {ch.database}.{mv_name}")
        context.log.debug(f"DDL:\n{mv_ddl}")
        clickhouse.execute(mv_ddl)

        context.log.info(f"✅ All ClickHouse objects created for '{name}'")

        return dg.MaterializeResult(
            metadata={
                "database": ch.database,
                "target_table": ch.table_name,
                "kafka_queue": queue_name,
                "materialized_view": mv_name,
                "engine": ch.engine,
            }
        )

    # ──────────────────────────────────────────────
    # Asset 4: Historical Data Load
    # ──────────────────────────────────────────────
    @dg.asset(
        name=f"historical_load_{name}",
        group_name=f"onboarding_{name}",
        deps=[dg.AssetKey(f"clickhouse_tables_{name}")],
        metadata={"dataset": name},
        description=f"Bulk load historical data from MongoDB into ClickHouse for dataset '{name}'",
    )
    def historical_load_asset(
        context,
        mongodb: MongoDBResource,
        clickhouse: ClickHouseResource,
    ) -> dg.MaterializeResult:
        ch = dataset.clickhouse
        mongo = dataset.source.mongodb
        full_table = f"{ch.database}.{ch.table_name}"

        # Check if target already has data (avoid double-load)
        existing_count = clickhouse.get_row_count(ch.database, ch.table_name)
        if existing_count > 0:
            context.log.info(
                f"ℹ️ Table '{full_table}' already has {existing_count} rows. "
                "Skipping historical load. Delete the table to re-load."
            )
            return dg.MaterializeResult(
                metadata={
                    "skipped": True,
                    "existing_rows": existing_count,
                }
            )

        # Count source documents
        total_docs = mongodb.count_documents(mongo.database, mongo.collection)
        context.log.info(
            f"📊 Loading {total_docs} documents from "
            f"MongoDB {mongo.database}.{mongo.collection} → {full_table}"
        )

        # Column names for ClickHouse insert (excluding CDC metadata for historical)
        column_names = [col.name for col in ch.columns]
        total_inserted = 0
        batch_num = 0

        for batch in mongodb.read_all_documents(
            mongo.database, mongo.collection, batch_size=5000
        ):
            batch_num += 1
            # Map MongoDB docs to ClickHouse rows
            ch_rows = []
            for doc in batch:
                row = {}
                for col in ch.columns:
                    if col.name == "_version":
                        row[col.name] = 1
                    elif col.name == "_deleted":
                        row[col.name] = 0
                    elif col.name in doc:
                        row[col.name] = doc[col.name]
                    elif col.default is not None:
                        row[col.name] = col.default
                    else:
                        row[col.name] = None
                ch_rows.append(row)

            inserted = clickhouse.insert_dataframe(
                full_table, ch_rows, column_names
            )
            total_inserted += inserted
            context.log.info(
                f"  Batch {batch_num}: inserted {inserted} rows "
                f"({total_inserted}/{total_docs} total)"
            )

        context.log.info(
            f"✅ Historical load complete: {total_inserted} rows loaded into {full_table}"
        )

        return dg.MaterializeResult(
            metadata={
                "source_collection": f"{mongo.database}.{mongo.collection}",
                "target_table": full_table,
                "total_documents": total_docs,
                "rows_inserted": total_inserted,
            }
        )

    return [
        kafka_topic_asset,
        debezium_connector_asset,
        clickhouse_tables_asset,
        historical_load_asset,
    ]


def build_all_dataset_assets(
    datasets_dir: str | None = None,
) -> list[dg.AssetsDefinition]:
    """Load all dataset YAMLs and generate all onboarding assets."""
    configs = load_dataset_configs(datasets_dir)
    all_assets = []
    for config in configs:
        all_assets.extend(build_dataset_assets(config))
    return all_assets
