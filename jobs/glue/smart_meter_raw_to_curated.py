"""AWS Glue Spark job: validate new Firehose JSON and append curated Parquet."""

from data_contracts import (
    SUPPORTED_SCHEMA_VERSIONS,
    US_CENSUS_REGIONS,
    US_STATE_CODES,
    US_STATE_REGION_KEYS,
    UUID_PATTERN,
)
from glue_lib import (
    BACKFILL_ARGUMENTS,
    BackfillWindow,
    ColumnMapping,
    GlueJob,
    ProcessingMode,
    S3LocationArguments,
    ValidationRule,
    write_partitioned_parquet,
)
from pyspark.sql import functions as F

REQUIRED_ARGUMENTS = [
    "processing_mode",
    *BACKFILL_ARGUMENTS,
]
MAPPINGS = [
    ColumnMapping("event_id", "event_id", "string"),
    ColumnMapping("schema_version", "schema_version", "string"),
    ColumnMapping("meter_id", "meter_id", "string"),
    ColumnMapping("us_region", "us_region", "string"),
    ColumnMapping("state_code", "state_code", "string"),
    ColumnMapping("event_time", "event_time", "timestamp"),
    ColumnMapping("energy_kwh", "energy_kwh", "double"),
    ColumnMapping("voltage_v", "voltage_v", "double"),
    ColumnMapping("current_a", "current_a", "double"),
    ColumnMapping("power_factor", "power_factor", "double"),
]
REQUIRED_COLUMNS = [mapping.target for mapping in MAPPINGS]
VALIDATION_RULES = [
    ValidationRule(
        "format:event_id_uuid", lambda: F.col("event_id").rlike(UUID_PATTERN)
    ),
    ValidationRule(
        "schema:supported_version",
        lambda: F.col("schema_version").isin(*SUPPORTED_SCHEMA_VERSIONS),
    ),
    ValidationRule(
        "domain:us_census_region",
        lambda: F.col("us_region").isin(*US_CENSUS_REGIONS),
    ),
    ValidationRule(
        "domain:us_state_code",
        lambda: F.col("state_code").isin(*US_STATE_CODES),
    ),
    ValidationRule(
        "domain:state_region_alignment",
        lambda: F.concat_ws(":", "state_code", "us_region").isin(*US_STATE_REGION_KEYS),
    ),
    ValidationRule("range:energy_kwh", lambda: F.col("energy_kwh") >= 0),
    ValidationRule("range:voltage_v", lambda: F.col("voltage_v") >= 0),
    ValidationRule("range:current_a", lambda: F.col("current_a") >= 0),
    ValidationRule("range:power_factor", lambda: F.col("power_factor").between(0, 1)),
]


def main() -> None:
    job = GlueJob(
        required_arguments=REQUIRED_ARGUMENTS,
        mappings=MAPPINGS,
        required_columns=REQUIRED_COLUMNS,
        validation_rules=VALIDATION_RULES,
        source=S3LocationArguments("raw_bucket", "raw_prefix"),
        target=S3LocationArguments("curated_bucket", "curated_prefix"),
        errors=S3LocationArguments("error_bucket", "error_prefix"),
    )
    runtime = job.runtime
    processing_mode = ProcessingMode.from_runtime(runtime)
    backfill_window = BackfillWindow.from_runtime(runtime, processing_mode)
    windows = backfill_window.batch_windows() if backfill_window else (None,)
    processed_batches = 0
    dynamic_frame_errors_quarantined = False
    quality_errors_quarantined = False

    for batch_number, window in enumerate(windows, start=1):
        context_suffix = f"batch_{batch_number}"
        raw = job.read_s3(
            uri=job.source_uri,
            data_format="json",
            format_options={"multiline": False},
            transformation_context=f"smart_meter_raw_events_{context_suffix}",
            backfill_window=window,
        )
        if raw is None:
            continue
        dynamic_frame_errors_quarantined |= job.quarantine_dynamic_frame_errors(
            raw,
            transformation_context=f"write_raw_dynamic_frame_errors_{context_suffix}",
        )
        raw_records = raw.toDF()
        if window is not None:
            raw_records = window.filter(raw_records, "event_time")
        validation = job.validate(
            raw_records,
            error_stage="smart_meter_raw_to_curated",
            additional_columns={
                "raw_bucket": F.lit(job.argument("raw_bucket")),
                "raw_object_key": F.regexp_replace(
                    F.input_file_name(),
                    rf"^s3a?://{job.argument('raw_bucket')}/",
                    "",
                ),
                "raw_line_number": F.lit(0).cast("long"),
                "record_source": F.input_file_name(),
            },
            deduplicate_by=("event_id",),
        )
        curated = validation.valid_records.select(
            "*",
            F.date_format("event_time", "yyyy").alias("year"),
            F.date_format("event_time", "MM").alias("month"),
            F.date_format("event_time", "dd").alias("day"),
        )
        write_partitioned_parquet(
            curated,
            job.target_uri,
            processing_mode,
            ("year", "month", "day"),
        )
        quality_errors_quarantined |= validation.rejected_records_written
        validation.release()
        processed_batches += 1

    runtime.emit_summary(
        curated_write_completed=True,
        dynamic_frame_errors_quarantined=dynamic_frame_errors_quarantined,
        quality_errors_quarantined=quality_errors_quarantined,
        processed_batches=processed_batches,
        processing_mode=processing_mode.value,
        source_uri=job.source_uri,
        target_uri=job.target_uri,
    )
    job.commit()


if __name__ == "__main__":
    main()
