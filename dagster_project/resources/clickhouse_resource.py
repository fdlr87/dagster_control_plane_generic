"""ClickHouse resource for DDL execution and data operations."""

from __future__ import annotations

from typing import Any

import clickhouse_connect
import dagster as dg
from clickhouse_connect.driver import Client


class ClickHouseResource(dg.ConfigurableResource):
    """Resource wrapping clickhouse-connect client for DDL and DML operations."""

    host: str = "clickhouse"
    port: int = 8123
    username: str = "default"
    password: str = ""

    def _get_client(self) -> Client:
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
        )

    def execute(self, query: str) -> None:
        """Execute a DDL/DML statement."""
        client = self._get_client()
        try:
            client.command(query)
        finally:
            client.close()

    def query(self, query: str) -> Any:
        """Execute a query and return results."""
        client = self._get_client()
        try:
            return client.query(query)
        finally:
            client.close()

    def insert_dataframe(self, table: str, data: list[dict], column_names: list[str]) -> int:
        """Insert a list of dicts as rows into a ClickHouse table.

        Returns the number of rows inserted.
        """
        if not data:
            return 0

        client = self._get_client()
        try:
            rows = []
            for doc in data:
                row = [doc.get(col) for col in column_names]
                rows.append(row)

            client.insert(
                table=table,
                data=rows,
                column_names=column_names,
            )
            return len(rows)
        finally:
            client.close()

    def table_exists(self, database: str, table: str) -> bool:
        """Check if a table exists in the given database."""
        client = self._get_client()
        try:
            result = client.command(
                f"SELECT count() FROM system.tables "
                f"WHERE database = '{database}' AND name = '{table}'"
            )
            return int(result) > 0
        finally:
            client.close()

    def get_row_count(self, database: str, table: str) -> int:
        """Get the row count of a table."""
        client = self._get_client()
        try:
            result = client.command(f"SELECT count() FROM {database}.{table}")
            return int(result)
        finally:
            client.close()
