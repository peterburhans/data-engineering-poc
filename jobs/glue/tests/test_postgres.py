"""Unit tests for transaction-scoped PostgreSQL partition loading."""

import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest
from glue_lib.postgres import PostgresConnection, TemporaryTableLoad, load_partition

CONNECTION = PostgresConnection("db", 5432, "warehouse", "user", "password", ssl=False)
LOAD = TemporaryTableLoad(
    create_sql="create temporary table stage (id text) on commit drop",
    insert_sql="insert into stage values (%s)",
    merge_sql="insert into target select * from stage on conflict do nothing",
    columns=("id",),
    batch_size=2,
)


def install_pg8000(monkeypatch: pytest.MonkeyPatch, connection: Mock) -> Mock:
    connect = Mock(return_value=connection)
    package = ModuleType("pg8000")
    package.__path__ = []
    dbapi = ModuleType("pg8000.dbapi")
    dbapi.connect = connect
    package.dbapi = dbapi
    monkeypatch.setitem(sys.modules, "pg8000", package)
    monkeypatch.setitem(sys.modules, "pg8000.dbapi", dbapi)
    return connect


def row(value: str) -> SimpleNamespace:
    return SimpleNamespace(asDict=lambda recursive=False: {"id": value})


def test_partition_uses_temp_table_batches_and_commits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = Mock(rowcount=2)
    connection = Mock()
    connection.cursor.return_value = cursor
    connect = install_pg8000(monkeypatch, connection)

    assert list(
        load_partition(iter([row("a"), row("b"), row("c")]), CONNECTION, LOAD)
    ) == [2]
    connect.assert_called_once_with(
        host="db",
        port=5432,
        database="warehouse",
        user="user",
        password="password",
        timeout=15,
        ssl_context=False,
    )
    assert cursor.executemany.call_count == 2
    cursor.executemany.assert_any_call(LOAD.insert_sql, [("a",), ("b",)])
    cursor.executemany.assert_any_call(LOAD.insert_sql, [("c",)])
    connection.commit.assert_called_once_with()
    connection.rollback.assert_not_called()
    connection.close.assert_called_once_with()


def test_partition_rolls_back_and_closes_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = Mock()
    cursor.executemany.side_effect = RuntimeError("database unavailable")
    connection = Mock()
    connection.cursor.return_value = cursor
    install_pg8000(monkeypatch, connection)

    with pytest.raises(RuntimeError, match="database unavailable"):
        list(load_partition(iter([row("a")]), CONNECTION, LOAD))

    connection.rollback.assert_called_once_with()
    connection.commit.assert_not_called()
    connection.close.assert_called_once_with()


def test_empty_partition_does_not_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = Mock()
    connect = install_pg8000(monkeypatch, connection)

    assert list(load_partition(iter(()), CONNECTION, LOAD)) == []
    connect.assert_not_called()


def test_batch_size_must_be_positive() -> None:
    with pytest.raises(ValueError, match="greater than zero"):
        TemporaryTableLoad("create", "insert", "merge", ("id",), batch_size=0)
