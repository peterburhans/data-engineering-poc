"""Real PySpark integration coverage for mapping and validation behavior."""

import os
from types import SimpleNamespace

import pytest

if os.environ.get("GLUE_SPARK_TESTS") != "1":
    pytest.skip(
        "set GLUE_SPARK_TESTS=1 with PySpark installed", allow_module_level=True
    )

from glue_lib import ColumnMapping, GlueJob, S3LocationArguments, ValidationRule
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def test_validation_casts_splits_deduplicates_and_reports_reasons(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark = (
        SparkSession.builder.master("local[2]").appName("glue-lib-tests").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    rejected = []
    runtime = SimpleNamespace(
        args={
            "source_bucket": "source",
            "source_prefix": "events",
            "error_bucket": "errors",
            "error_prefix": "quarantine",
        },
        optional_argument=lambda _name: None,
    )
    monkeypatch.setattr("glue_lib.job.GlueRuntime.create", lambda _arguments: runtime)
    monkeypatch.setattr(
        GlueJob,
        "_write_rejected_records",
        lambda _self, records, **_kwargs: (
            rejected.extend(records.collect()) or True
        ),
    )
    job = GlueJob(
        required_arguments=(),
        mappings=(
            ColumnMapping("event_id", "event_id", "string"),
            ColumnMapping("schema_version", "schema_version", "string"),
            ColumnMapping("energy", "energy", "double"),
        ),
        required_columns=("event_id", "schema_version", "energy"),
        validation_rules=(
            ValidationRule(
                "schema:supported", lambda: F.col("schema_version") == "1.0"
            ),
            ValidationRule("range:energy", lambda: F.col("energy") >= 0),
        ),
        source=S3LocationArguments("source_bucket", "source_prefix"),
        errors=S3LocationArguments("error_bucket", "error_prefix"),
    )
    frame = spark.createDataFrame(
        [
            ("one", "1.0", "2.5"),
            ("one", "1.0", "2.5"),
            ("two", "2.0", "3.0"),
            ("three", "1.0", "-1.0"),
            ("four", "1.0", "not-a-number"),
        ],
        ("event_id", "schema_version", "energy"),
    )

    outcome = job.validate(frame, error_stage="test", deduplicate_by=("event_id",))
    valid = outcome.valid_records.collect()
    outcome.release()
    spark.stop()

    assert [(item.event_id, item.energy) for item in valid] == [("one", 2.5)]
    assert outcome.rejected_records_written is True
    assert len(rejected) == 3
    reasons = {item.event_id: set(item.error_reasons) for item in rejected}
    assert reasons["two"] == {"schema:supported"}
    assert reasons["three"] == {"range:energy"}
    assert reasons["four"] == {"required:energy", "range:energy"}
