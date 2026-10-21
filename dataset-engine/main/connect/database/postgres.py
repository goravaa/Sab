from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from uuid import uuid4

from main.schema.service import DatabaseConfig


class PostgresConnector:
    """Read-only operations needed to inspect and scan a PostgreSQL source."""

    def __init__(self, config: DatabaseConfig, *, connect_timeout: int = 10) -> None:
        if config.type != "postgres":
            raise ValueError("PostgresConnector requires a postgres DatabaseConfig")
        self.config = config
        self.connect_timeout = connect_timeout

    def _connect(self):
        """Open a psycopg 3 connection, importing the optional driver lazily."""
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError(
                "PostgreSQL support requires psycopg 3; install the 'psycopg[binary]' package"
            ) from exc

        connection = psycopg.connect(
            host=self.config.host,
            port=self.config.port,
            dbname=self.config.database,
            user=self.config.username,
            password=self.config.password.get_secret_value(),
            connect_timeout=self.connect_timeout,
            row_factory=dict_row,
        )
        # The engine only inspects source data; each connection is explicitly
        # placed in read-only mode to guard against accidental writes.
        connection.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        return connection

    @staticmethod
    def _qualified_table(table: str) -> tuple[str, str]:
        """Return schema and table, treating an unqualified name as public."""
        parts = table.split(".")
        if len(parts) == 1 and all(parts):
            return "public", parts[0]
        if len(parts) == 2 and all(parts):
            return parts[0], parts[1]
        raise ValueError("table must be a table name or schema.table")

    @staticmethod
    def _identifier(schema: str, table: str):
        # Identifiers are never interpolated into SQL as plain strings.
        from psycopg import sql

        return sql.Identifier(schema, table)

    def check_connection(self) -> None:
        """Raise the driver's connection error if the source cannot be reached."""
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()

    def list_tables(self) -> list[str]:
        """List ordinary tables in non-system schemas, honoring configured tables."""
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                  AND table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name
                """
            ).fetchall()

        available = [f"{row['table_schema']}.{row['table_name']}" for row in rows]
        if self.config.tables is None:
            return available

        # Preserve the user's configured names while filtering out nonexistent
        # tables. This lets callers report an invalid selection separately.
        available_set = set(available)
        result: list[str] = []
        for configured in self.config.tables:
            schema, name = self._qualified_table(configured)
            canonical = f"{schema}.{name}"
            if canonical in available_set:
                result.append(configured)
        return result

    def primary_key_columns(self, table: str) -> list[str]:
        """Return primary key columns in key order; an empty list means no PK."""
        schema, name = self._qualified_table(table)
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT attribute.attname AS column_name
                FROM pg_index AS index_info
                JOIN pg_class AS relation ON relation.oid = index_info.indrelid
                JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
                JOIN unnest(index_info.indkey) WITH ORDINALITY AS key_column(attnum, position)
                  ON TRUE
                JOIN pg_attribute AS attribute
                  ON attribute.attrelid = relation.oid
                 AND attribute.attnum = key_column.attnum
                WHERE index_info.indisprimary
                  AND namespace.nspname = %s
                  AND relation.relname = %s
                ORDER BY key_column.position
                """,
                (schema, name),
            ).fetchall()
        return [row["column_name"] for row in rows]

    def count_rows(self, table: str) -> int:
        """Return an exact row count for a table."""
        from psycopg import sql

        schema, name = self._qualified_table(table)
        with self._connect() as connection:
            row = connection.execute(
                sql.SQL("SELECT count(*) AS row_count FROM {} ").format(
                    self._identifier(schema, name)
                )
            ).fetchone()
        return int(row["row_count"])

    def iter_rows(self, table: str, *, batch_size: int = 1000) -> Iterator[dict[str, Any]]:
        """Yield table rows in primary-key order using a server-side cursor.

        A stable key order allows a scan to be repeated deterministically. Tables
        without a primary key are rejected because the engine cannot safely
        create stable document identifiers for their rows.
        """
        if batch_size < 1:
            raise ValueError("batch_size must be greater than zero")

        from psycopg import sql

        schema, name = self._qualified_table(table)
        keys = self.primary_key_columns(table)
        if not keys:
            raise ValueError(f"table {table!r} has no primary key; stable row IDs are required")

        ordering = sql.SQL(", ").join(sql.Identifier(key) for key in keys)
        query = sql.SQL("SELECT * FROM {} ORDER BY {}").format(
            self._identifier(schema, name), ordering
        )

        with self._connect() as connection:
            # Named cursors stream rows from PostgreSQL instead of loading the
            # full table into the worker's memory.
            with connection.cursor(name=f"sab_scan_{uuid4().hex}") as cursor:
                cursor.itersize = batch_size
                cursor.execute(query)
                while rows := cursor.fetchmany(batch_size):
                    yield from rows
