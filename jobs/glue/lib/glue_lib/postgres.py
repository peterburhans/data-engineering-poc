"""Distributed PostgreSQL loading through transaction-scoped temporary tables."""

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from itertools import chain, islice
from typing import Any


@dataclass(frozen=True)
class PostgresConnection:
    host: str
    port: int
    database: str
    username: str
    password: str
    ssl: bool = True
    connect_timeout_seconds: int = 15
    statement_timeout_seconds: int = 300


@dataclass(frozen=True)
class TemporaryTableLoad:
    """SQL contract used independently by every Spark partition."""

    create_sql: str
    insert_sql: str
    merge_sql: str
    columns: Sequence[str]
    batch_size: int = 1_000

    def __post_init__(self) -> None:
        if self.batch_size < 1:
            raise ValueError("batch_size must be greater than zero")


def _batches(
    rows: Iterator[tuple[Any, ...]], size: int
) -> Iterator[list[tuple[Any, ...]]]:
    while batch := list(islice(rows, size)):
        yield batch


def load_partition(
    rows: Iterator[Any],
    connection_config: PostgresConnection,
    load: TemporaryTableLoad,
) -> Iterator[int]:
    """Load one Spark partition and yield its committed insert count."""

    first = next(rows, None)
    if first is None:
        return

    import pg8000.dbapi

    connection = pg8000.dbapi.connect(
        host=connection_config.host,
        port=connection_config.port,
        database=connection_config.database,
        user=connection_config.username,
        password=connection_config.password,
        timeout=connection_config.connect_timeout_seconds,
        ssl_context=connection_config.ssl,
    )
    try:
        cursor = connection.cursor()
        try:
            timeout_ms = connection_config.statement_timeout_seconds * 1_000
            cursor.execute(f"set local statement_timeout = {timeout_ms}")
            cursor.execute(load.create_sql)
            dictionaries = (
                row.asDict(recursive=False) for row in chain((first,), rows)
            )
            values = (
                tuple(row[column] for column in load.columns) for row in dictionaries
            )
            for batch in _batches(values, load.batch_size):
                cursor.executemany(load.insert_sql, batch)
            cursor.execute(load.merge_sql)
            inserted = max(cursor.rowcount, 0)
            connection.commit()
            yield inserted
        finally:
            cursor.close()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def load_dataframe(
    frame: Any,
    connection: PostgresConnection,
    load: TemporaryTableLoad,
) -> int:
    """Load all Spark partitions without collecting their records on the driver."""

    counts = frame.rdd.mapPartitions(
        lambda rows: load_partition(rows, connection, load)
    ).collect()
    return sum(counts)
