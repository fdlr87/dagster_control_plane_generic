"""Debezium / Kafka Connect resource for connector management."""

from __future__ import annotations

import time
from typing import Any

import dagster as dg
import requests


class DebeziumResource(dg.ConfigurableResource):
    """Resource wrapping Kafka Connect REST API for Debezium connector management."""

    connect_url: str = "http://kafka-connect:8083"
    request_timeout: int = 30

    @property
    def _base_url(self) -> str:
        return self.connect_url.rstrip("/")

    def connector_exists(self, connector_name: str) -> bool:
        """Check if a connector already exists."""
        try:
            resp = requests.get(
                f"{self._base_url}/connectors/{connector_name}",
                timeout=self.request_timeout,
            )
            return resp.status_code == 200
        except requests.RequestException:
            return False

    def create_connector(self, connector_name: str, config: dict[str, Any]) -> dict:
        """Create a new Debezium connector.

        Returns the connector status from the API.
        """
        payload = {
            "name": connector_name,
            "config": config,
        }
        resp = requests.post(
            f"{self._base_url}/connectors",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=self.request_timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def delete_connector(self, connector_name: str) -> bool:
        """Delete a connector. Returns True if deleted."""
        resp = requests.delete(
            f"{self._base_url}/connectors/{connector_name}",
            timeout=self.request_timeout,
        )
        return resp.status_code in (200, 204)

    def get_connector_status(self, connector_name: str) -> dict:
        """Get the status of a connector."""
        resp = requests.get(
            f"{self._base_url}/connectors/{connector_name}/status",
            timeout=self.request_timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def wait_for_connector_running(
        self, connector_name: str, timeout_seconds: int = 60
    ) -> bool:
        """Wait until a connector reaches RUNNING state."""
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            try:
                status = self.get_connector_status(connector_name)
                connector_state = status.get("connector", {}).get("state", "")
                if connector_state == "RUNNING":
                    tasks = status.get("tasks", [])
                    if tasks and all(t.get("state") == "RUNNING" for t in tasks):
                        return True
            except Exception:
                pass
            time.sleep(3)
        return False

    def list_connectors(self) -> list[str]:
        """List all registered connectors."""
        resp = requests.get(
            f"{self._base_url}/connectors",
            timeout=self.request_timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def build_mongodb_connector_config(
        self,
        connector_name: str,
        mongodb_uri: str,
        database: str,
        collection: str,
        topic_prefix: str,
    ) -> dict[str, str]:
        """Build a Debezium MongoDB connector configuration dict."""
        return {
            "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
            "mongodb.connection.string": mongodb_uri,
            "topic.prefix": topic_prefix,
            "collection.include.list": f"{database}.{collection}",
            # Flatten the nested Debezium envelope to get the document state
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState",
            "transforms.unwrap.drop.tombstones": "false",
            "transforms.unwrap.delete.handling.mode": "rewrite",
            "transforms.unwrap.add.fields": "op,source.ts_ms",
            # Snapshot: initial = full dump then stream
            "snapshot.mode": "initial",
            # Converters — plain JSON, no schemas
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            # Heartbeat to keep offsets fresh
            "heartbeat.interval.ms": "10000",
        }
