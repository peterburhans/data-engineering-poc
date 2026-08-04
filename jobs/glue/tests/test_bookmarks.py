"""Unit tests for local object-level bookmarks."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from glue_lib.bookmarks import LocalObjectBookmarks


def runtime(**arguments: str) -> SimpleNamespace:
    return SimpleNamespace(
        endpoint_url="http://localstack:4566",
        optional_argument=lambda name: arguments.get(name),
        run_id="test-run",
    )


def test_bookmark_arguments_must_be_supplied_together() -> None:
    with pytest.raises(ValueError, match="must be supplied together"):
        LocalObjectBookmarks.from_runtime(
            runtime(**{"local-bookmark-table": "bookmarks"})
        )


def test_unprocessed_s3_objects_excludes_committed_objects(monkeypatch) -> None:
    table = Mock()
    table.query.return_value = {"Items": [{"object_uri": "s3://raw/events/one.json"}]}
    paginator = Mock()
    paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "events/one.json"},
                {"Key": "events/two.json"},
            ]
        }
    ]
    client = Mock()
    client.get_paginator.return_value = paginator
    monkeypatch.setattr(LocalObjectBookmarks, "table", property(lambda _self: table))
    monkeypatch.setattr("glue_lib.bookmarks.boto3.client", lambda *_args, **_kwargs: client)

    bookmarks = LocalObjectBookmarks(runtime(), "bookmarks", "raw_to_curated")

    assert bookmarks.unprocessed_s3_objects("s3://raw/events/", "json") == [
        "s3://raw/events/two.json"
    ]


def test_commit_records_each_processed_object(monkeypatch) -> None:
    batch = Mock()
    batch.__enter__ = Mock(return_value=batch)
    batch.__exit__ = Mock(return_value=False)
    table = Mock()
    table.batch_writer.return_value = batch
    monkeypatch.setattr(LocalObjectBookmarks, "table", property(lambda _self: table))
    bookmarks = LocalObjectBookmarks(runtime(), "bookmarks", "raw_to_curated")

    bookmarks.commit(["s3://raw/events/one.json"])

    item = batch.put_item.call_args.kwargs["Item"]
    assert item["stage"] == "raw_to_curated"
    assert item["object_uri"] == "s3://raw/events/one.json"
    assert item["job_run_id"] == "test-run"
