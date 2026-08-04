"""Unit tests for the generic Glue job definition and S3 locations."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from glue_lib import ColumnMapping, GlueJob, S3LocationArguments, ValidationRule

SOURCE = S3LocationArguments("input_bucket", "input_prefix")
TARGET = S3LocationArguments("output_bucket", "output_prefix")
ERRORS = S3LocationArguments("quarantine_bucket", "quarantine_prefix")
MAPPINGS = [ColumnMapping("source_id", "id", "string")]


def build_job(monkeypatch: pytest.MonkeyPatch, **overrides: object) -> GlueJob:
    arguments = {
        "input_bucket": "s3://input-bucket/",
        "input_prefix": "/incoming/events/",
        "output_bucket": "output-bucket",
        "output_prefix": "curated/events",
        "quarantine_bucket": "error-bucket",
        "quarantine_prefix": "/quarantine/",
    }
    runtime = SimpleNamespace(args=arguments, optional_argument=lambda _name: None)
    create = Mock(return_value=runtime)
    monkeypatch.setattr("glue_lib.job.GlueRuntime.create", create)
    configuration = {
        "required_arguments": ["processing_mode", "input_bucket"],
        "mappings": MAPPINGS,
        "required_columns": ["id"],
        "source": SOURCE,
        "target": TARGET,
        "errors": ERRORS,
    }
    configuration.update(overrides)
    job = GlueJob(**configuration)
    job._runtime_create = create  # type: ignore[attr-defined]
    return job


@pytest.mark.parametrize(
    ("bucket", "prefix", "expected"),
    [
        ("events", "raw", "s3://events/raw/"),
        ("s3://events/", "/raw/events/", "s3://events/raw/events/"),
        ("events", "", "s3://events/"),
    ],
)
def test_s3_uri_normalization(bucket: str, prefix: str, expected: str) -> None:
    assert GlueJob._s3_uri(bucket, prefix) == expected


def test_s3_uri_rejects_empty_bucket() -> None:
    with pytest.raises(ValueError, match="S3 bucket must not be empty"):
        GlueJob._s3_uri("///", "events")


def test_locations_are_derived_from_registered_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = build_job(monkeypatch)

    assert job.source_uri == "s3://input-bucket/incoming/events/"
    assert job.target_uri == "s3://output-bucket/curated/events/"
    assert job.error_uri == "s3://error-bucket/quarantine/"
    job._runtime_create.assert_called_once_with(  # type: ignore[attr-defined]
        [
            "processing_mode",
            "input_bucket",
            "input_prefix",
            "quarantine_bucket",
            "quarantine_prefix",
            "output_bucket",
            "output_prefix",
        ]
    )


def test_target_uri_requires_a_configured_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = build_job(monkeypatch, target=None)

    with pytest.raises(ValueError, match="does not define an S3 target"):
        _ = job.target_uri


def test_missing_location_argument_has_clear_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = build_job(monkeypatch)
    del job.args["quarantine_prefix"]

    with pytest.raises(ValueError, match="'quarantine_prefix' is not configured"):
        _ = job.error_uri


def test_duplicate_mapping_targets_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mappings = [*MAPPINGS, ColumnMapping("other_id", "id", "string")]

    with pytest.raises(ValueError, match="duplicate targets: id"):
        build_job(monkeypatch, mappings=mappings)


def test_unknown_required_columns_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="not mapping targets: missing"):
        build_job(monkeypatch, required_columns=["id", "missing"])


def test_duplicate_validation_rule_names_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rules = [
        ValidationRule("valid", lambda: None),
        ValidationRule("valid", lambda: None),
    ]

    with pytest.raises(ValueError, match="rule names must be unique"):
        build_job(monkeypatch, validation_rules=rules)


def test_empty_rejected_frame_does_not_create_quarantine_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = build_job(monkeypatch)
    records = Mock()
    records.isEmpty.return_value = True

    written = job._write_rejected_records(records, error_type="quality")

    assert written is False
    records.write.mode.assert_not_called()


def test_nonempty_rejected_frame_is_written_to_quarantine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = build_job(monkeypatch)
    records = Mock()
    records.isEmpty.return_value = False
    destination = "s3://error-bucket/quarantine/errors/"
    monkeypatch.setattr(job, "quarantine_uri", lambda *_args: destination)

    written = job._write_rejected_records(records, error_type="quality")

    assert written is True
    records.write.mode.assert_called_once_with("append")
    records.write.mode.return_value.json.assert_called_once_with(destination)
