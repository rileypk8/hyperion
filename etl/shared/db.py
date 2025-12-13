"""Database connection utilities for Hyperion ETL."""

import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor
from sqlalchemy import create_engine
from contextlib import contextmanager
from typing import Generator, Any

from .config import settings


def get_engine():
    """Create SQLAlchemy engine."""
    return create_engine(settings.postgres_url, echo=False)


@contextmanager
def get_db_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Get a database connection as a context manager."""
    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password,
        dbname=settings.postgres_db,
    )
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_cursor(conn: psycopg2.extensions.connection, dict_cursor: bool = False):
    """Get a cursor as a context manager."""
    cursor_factory = RealDictCursor if dict_cursor else None
    cursor = conn.cursor(cursor_factory=cursor_factory)
    try:
        yield cursor
    finally:
        cursor.close()


def upsert_batch(
    conn: psycopg2.extensions.connection,
    table: str,
    records: list[dict],
    conflict_columns: list[str],
    update_columns: list[str] | None = None,
) -> int:
    """
    Batch upsert records into a table.

    Args:
        conn: Database connection
        table: Target table name
        records: List of dictionaries to insert
        conflict_columns: Columns to use for conflict detection
        update_columns: Columns to update on conflict (None = do nothing)

    Returns:
        Number of records processed
    """
    if not records:
        return 0

    columns = list(records[0].keys())
    placeholders = ", ".join(["%s"] * len(columns))
    column_names = ", ".join(columns)
    conflict_cols = ", ".join(conflict_columns)

    if update_columns:
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        on_conflict = f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_clause}"
    else:
        on_conflict = f"ON CONFLICT ({conflict_cols}) DO NOTHING"

    sql = f"""
        INSERT INTO {table} ({column_names})
        VALUES ({placeholders})
        {on_conflict}
    """

    with get_cursor(conn) as cursor:
        execute_batch(
            cursor,
            sql,
            [tuple(r[col] for col in columns) for r in records],
            page_size=settings.batch_size,
        )
        conn.commit()

    return len(records)


def get_lookup_id(
    conn: psycopg2.extensions.connection,
    table: str,
    name_column: str,
    value: str,
    insert_if_missing: bool = True,
) -> int | None:
    """
    Get ID from a lookup table by name, optionally inserting if missing.

    Args:
        conn: Database connection
        table: Lookup table name
        name_column: Column containing the name
        value: Value to look up
        insert_if_missing: Whether to insert if not found

    Returns:
        ID of the record, or None if not found and not inserting
    """
    with get_cursor(conn) as cursor:
        cursor.execute(
            f"SELECT id FROM {table} WHERE {name_column} = %s",
            (value,)
        )
        result = cursor.fetchone()

        if result:
            return result[0]

        if insert_if_missing:
            cursor.execute(
                f"INSERT INTO {table} ({name_column}) VALUES (%s) RETURNING id",
                (value,)
            )
            conn.commit()
            return cursor.fetchone()[0]

        return None


def get_or_create_lookup_ids(
    conn: psycopg2.extensions.connection,
    table: str,
    name_column: str,
    values: set[str],
) -> dict[str, int]:
    """
    Get or create multiple lookup IDs at once.

    Args:
        conn: Database connection
        table: Lookup table name
        name_column: Column containing the name
        values: Set of values to look up/create

    Returns:
        Dictionary mapping values to their IDs
    """
    if not values:
        return {}

    result = {}
    values_list = list(values)

    with get_cursor(conn) as cursor:
        # First, get existing IDs
        cursor.execute(
            f"SELECT id, {name_column} FROM {table} WHERE {name_column} = ANY(%s)",
            (values_list,)
        )
        for row in cursor.fetchall():
            result[row[1]] = row[0]

        # Insert missing values
        missing = [v for v in values_list if v not in result]
        if missing:
            from psycopg2.extras import execute_values
            inserted = execute_values(
                cursor,
                f"INSERT INTO {table} ({name_column}) VALUES %s "
                f"ON CONFLICT ({name_column}) DO NOTHING RETURNING id, {name_column}",
                [(v,) for v in missing],
                fetch=True,
            )
            for row in inserted:
                result[row[1]] = row[0]

            # Re-fetch any that were conflicts
            still_missing = [v for v in missing if v not in result]
            if still_missing:
                cursor.execute(
                    f"SELECT id, {name_column} FROM {table} WHERE {name_column} = ANY(%s)",
                    (still_missing,)
                )
                for row in cursor.fetchall():
                    result[row[1]] = row[0]

        conn.commit()

    return result
