"""Kafka Admin resource for topic management."""

from __future__ import annotations

import dagster as dg
from confluent_kafka.admin import AdminClient, NewTopic


class KafkaAdminResource(dg.ConfigurableResource):
    """Resource wrapping Kafka AdminClient for topic creation and management."""

    bootstrap_servers: str = "kafka:29092"

    def _get_client(self) -> AdminClient:
        return AdminClient({"bootstrap.servers": self.bootstrap_servers})

    def topic_exists(self, topic_name: str) -> bool:
        """Check if a topic already exists."""
        client = self._get_client()
        metadata = client.list_topics(timeout=10)
        return topic_name in metadata.topics

    def create_topic(
        self,
        topic_name: str,
        num_partitions: int = 3,
        replication_factor: int = 1,
    ) -> bool:
        """Create a Kafka topic. Returns True if created, False if already exists."""
        if self.topic_exists(topic_name):
            return False

        client = self._get_client()
        new_topic = NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
        futures = client.create_topics([new_topic])

        for topic, future in futures.items():
            future.result()  # Raises on error

        return True

    def list_topics(self) -> list[str]:
        """List all topics in the cluster."""
        client = self._get_client()
        metadata = client.list_topics(timeout=10)
        return [t for t in metadata.topics if not t.startswith("_")]
