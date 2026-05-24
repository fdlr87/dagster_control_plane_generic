"""MongoDB resource for reading data and collection metadata."""

from __future__ import annotations

from typing import Any, Generator

import dagster as dg
from pymongo import MongoClient
from pymongo.collection import Collection


class MongoDBResource(dg.ConfigurableResource):
    """Resource wrapping PyMongo client for read operations."""

    uri: str = "mongodb://mongodb:27017/?replicaSet=rs0&directConnection=true"
    default_database: str = "app_db"

    def _get_client(self) -> MongoClient:
        return MongoClient(self.uri)

    def get_collection(self, database: str, collection: str) -> Collection:
        """Get a reference to a MongoDB collection."""
        client = self._get_client()
        return client[database][collection]

    def count_documents(self, database: str, collection: str) -> int:
        """Count documents in a collection."""
        coll = self.get_collection(database, collection)
        return coll.estimated_document_count()

    def read_all_documents(
        self,
        database: str,
        collection: str,
        batch_size: int = 5000,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Read all documents from a collection in batches.

        Yields lists of dicts, each batch up to batch_size documents.
        """
        client = self._get_client()
        try:
            coll = client[database][collection]
            batch: list[dict[str, Any]] = []
            for doc in coll.find({}, batch_size=batch_size):
                # Convert ObjectId to string
                doc["_id"] = str(doc["_id"])
                batch.append(doc)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch
        finally:
            client.close()

    def list_collections(self, database: str) -> list[str]:
        """List all collections in a database."""
        client = self._get_client()
        try:
            return client[database].list_collection_names()
        finally:
            client.close()
