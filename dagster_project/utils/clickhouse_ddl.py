"""Dynamic ClickHouse DDL generation from dataset YAML configuration."""

from __future__ import annotations

from dagster_project.models.dataset_config import ClickHouseConfig, DatasetDefinition


def generate_create_database(database: str) -> str:
    """Generate CREATE DATABASE IF NOT EXISTS statement."""
    return f"CREATE DATABASE IF NOT EXISTS {database}"


def generate_target_table_ddl(config: DatasetDefinition) -> str:
    """Generate the ReplacingMergeTree target table DDL.

    This is the final destination table that stores the latest state
    of each document, with deduplication via the version column.
    """
    ch = config.clickhouse
    columns_sql = _build_columns_sql(ch)
    order_by = ", ".join(ch.order_by)
    version = ch.version_column

    return f"""CREATE TABLE IF NOT EXISTS {ch.database}.{ch.table_name} (
{columns_sql}
) ENGINE = {ch.engine}({version})
ORDER BY ({order_by})"""


def generate_kafka_queue_ddl(
    config: DatasetDefinition,
    kafka_brokers: str = "kafka:29092",
    consumer_group: str = "",
) -> str:
    """Generate the Kafka engine queue table DDL.

    This table matches the Debezium ExtractNewDocumentState output format:
    - Business columns without DEFAULT expressions (Kafka engine restriction)
    - __deleted (UInt8): Debezium delete flag
    - __op (String): Debezium operation type (r=read/snapshot, c=create, u=update, d=delete)
    - __source_ts_ms (UInt64): Debezium source timestamp in milliseconds
    """
    ch = config.clickhouse
    topic = config.kafka.topic_name
    queue_table = f"kafka_queue_{config.name}"

    if not consumer_group:
        consumer_group = f"clickhouse_{config.name}"

    # Build columns for the Kafka queue: business columns (no defaults) + Debezium metadata
    business_cols = _build_columns_sql(ch, include_defaults=False, exclude_cdc_columns=True)

    return f"""CREATE TABLE IF NOT EXISTS {ch.database}.{queue_table} (
{business_cols},
    __deleted UInt8,
    __op String,
    __source_ts_ms UInt64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = '{kafka_brokers}',
    kafka_topic_list = '{topic}',
    kafka_group_name = '{consumer_group}',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536"""


def generate_materialized_view_ddl(config: DatasetDefinition) -> str:
    """Generate the Materialized View that pipes Kafka queue → target table.

    Maps Debezium metadata fields to target table columns:
    - __source_ts_ms → _version (for ReplacingMergeTree deduplication)
    - __deleted → _deleted (soft-delete flag: 0=active, 1=deleted)
    """
    ch = config.clickhouse
    queue_table = f"kafka_queue_{config.name}"
    mv_name = f"mv_{config.name}"

    # Build SELECT column list, mapping Debezium fields to target columns
    select_cols = []
    for col in ch.columns:
        if col.name == "_version":
            select_cols.append("__source_ts_ms AS _version")
        elif col.name == "_deleted":
            select_cols.append("__deleted AS _deleted")
        else:
            select_cols.append(col.name)

    select_sql = ",\n    ".join(select_cols)

    return f"""CREATE MATERIALIZED VIEW IF NOT EXISTS {ch.database}.{mv_name}
TO {ch.database}.{ch.table_name}
AS SELECT
    {select_sql}
FROM {ch.database}.{queue_table}"""


def generate_drop_all_ddl(config: DatasetDefinition) -> list[str]:
    """Generate DROP statements for all objects (useful for teardown/reset)."""
    ch = config.clickhouse
    mv_name = f"mv_{config.name}"
    queue_table = f"kafka_queue_{config.name}"

    return [
        f"DROP VIEW IF EXISTS {ch.database}.{mv_name}",
        f"DROP TABLE IF EXISTS {ch.database}.{queue_table}",
        f"DROP TABLE IF EXISTS {ch.database}.{ch.table_name}",
    ]


def _build_columns_sql(
    ch: ClickHouseConfig,
    include_defaults: bool = True,
    exclude_cdc_columns: bool = False,
) -> str:
    """Build the column definitions SQL string.

    Args:
        ch: ClickHouse configuration
        include_defaults: Whether to include DEFAULT expressions
        exclude_cdc_columns: If True, skip _version and _deleted columns
            (used for Kafka queue tables where Debezium metadata replaces them)
    """
    lines = []
    for col in ch.columns:
        if exclude_cdc_columns and col.name in ("_version", "_deleted"):
            continue
        line = f"    {col.name} {col.type}"
        if include_defaults and col.default is not None:
            line += f" DEFAULT {col.default}"
        lines.append(line)
    return ",\n".join(lines)
