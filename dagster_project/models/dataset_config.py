"""Pydantic models for validating dataset YAML configuration."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, model_validator


class MongoDBSourceConfig(BaseModel):
    """MongoDB source configuration."""
    database: str
    collection: str


class SourceConfig(BaseModel):
    """Data source configuration."""
    mongodb: MongoDBSourceConfig


class KafkaConfig(BaseModel):
    """Kafka topic configuration."""
    topic_name: str = ""
    partitions: int = Field(default=3, ge=1)
    replication_factor: int = Field(default=1, ge=1)


class ColumnDefinition(BaseModel):
    """Single column definition for ClickHouse."""
    name: str
    type: str
    default: Optional[str] = None


class ClickHouseConfig(BaseModel):
    """ClickHouse destination configuration."""
    database: str = "analytics"
    table_name: str
    engine: str = "ReplacingMergeTree"
    order_by: list[str] = Field(default_factory=lambda: ["_id"])
    version_column: str = "_version"
    columns: list[ColumnDefinition]


class DatasetDefinition(BaseModel):
    """Full dataset definition parsed from YAML."""
    name: str
    source: SourceConfig
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    clickhouse: ClickHouseConfig

    @model_validator(mode="after")
    def auto_generate_topic_name(self) -> "DatasetDefinition":
        """Auto-generate Kafka topic name if not provided."""
        if not self.kafka.topic_name:
            db = self.source.mongodb.database
            col = self.source.mongodb.collection
            self.kafka.topic_name = f"cdc.{db}.{col}"
        return self


class DatasetYAML(BaseModel):
    """Root model wrapping the dataset key from the YAML file."""
    dataset: DatasetDefinition
