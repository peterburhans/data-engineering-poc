"""AWS Glue Spark job: idempotently merge curated Parquet into the warehouse."""

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
    PostgresConnection,
    ProcessingMode,
    S3LocationArguments,
    TemporaryTableLoad,
    ValidationRule,
    load_dataframe,
)
from pyspark.sql import functions as F

REQUIRED_ARGUMENTS = [
    "warehouse_secret_id",
    "warehouse_ssl_enabled",
    "warehouse_connect_timeout",
    "warehouse_statement_timeout",
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
    ColumnMapping("raw_bucket", "bucket_name", "string"),
    ColumnMapping("raw_object_key", "object_key", "string"),
    ColumnMapping("raw_line_number", "source_line_number", "int"),
    ColumnMapping("record_source", "record_source", "string"),
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
]
REQUIRED_SECRET_FIELDS = {"host", "port", "database", "username", "password"}
WAREHOUSE_COLUMNS = tuple(mapping.target for mapping in MAPPINGS)
TEMPORARY_TABLE_LOAD = TemporaryTableLoad(
    create_sql="""
        create temporary table ingest_meter_events (
            event_id text, schema_version text, meter_id text, us_region text,
            state_code text, event_time timestamp,
            energy_kwh double precision, voltage_v double precision,
            current_a double precision, power_factor double precision,
            bucket_name text, object_key text, source_line_number integer,
            record_source text
        ) on commit drop
    """,
    insert_sql=f"""
        insert into ingest_meter_events ({", ".join(WAREHOUSE_COLUMNS)})
        values ({", ".join(["%s"] * len(WAREHOUSE_COLUMNS))})
    """,
    merge_sql="""
        insert into core_raw.smart_meter_events (
            event_id, schema_version, meter_id, us_region, state_code,
            event_time, energy_kwh,
            voltage_v, current_a, power_factor, bucket_name,
            object_key, source_line_number, record_source
        )
        select
            event_id::uuid, schema_version, meter_id, us_region, state_code,
            event_time, energy_kwh,
            voltage_v, current_a, power_factor, bucket_name,
            object_key, source_line_number, record_source
        from ingest_meter_events
        on conflict (event_id) do nothing
    """,
    columns=WAREHOUSE_COLUMNS,
)


def main() -> None:
    job = GlueJob(
        required_arguments=REQUIRED_ARGUMENTS,
        mappings=MAPPINGS,
        required_columns=REQUIRED_COLUMNS,
        validation_rules=VALIDATION_RULES,
        source=S3LocationArguments("curated_bucket", "curated_prefix"),
        errors=S3LocationArguments("error_bucket", "error_prefix"),
    )
    runtime = job.runtime
    processing_mode = ProcessingMode.from_runtime(runtime)
    backfill_window = BackfillWindow.from_runtime(runtime, processing_mode)
    secret = runtime.read_json_secret(
        job.argument("warehouse_secret_id"), REQUIRED_SECRET_FIELDS
    )
    connection = PostgresConnection(
        host=secret["host"],
        port=int(secret["port"]),
        database=secret["database"],
        username=secret["username"],
        password=secret["password"],
        ssl=job.argument("warehouse_ssl_enabled").lower() == "true",
        connect_timeout_seconds=int(job.argument("warehouse_connect_timeout")),
        statement_timeout_seconds=int(job.argument("warehouse_statement_timeout")),
    )
    windows = backfill_window.batch_windows() if backfill_window else (None,)
    inserted = processed_batches = 0
    dynamic_frame_errors_quarantined = validation_errors_quarantined = False
    for batch_number, window in enumerate(windows, start=1):
        context = f"batch_{batch_number}"
        frame = job.read_s3(
            uri=job.source_uri,
            data_format="parquet",
            transformation_context=f"smart_meter_curated_events_{context}",
            backfill_window=window,
        )
        if frame is None:
            continue
        dynamic_frame_errors_quarantined |= job.quarantine_dynamic_frame_errors(
            frame, transformation_context=f"write_curated_errors_{context}"
        )
        records = frame.toDF()
        if window is not None:
            records = window.filter(records, "event_time")
        validation = job.validate(
            records,
            error_stage="smart_meter_curated_to_warehouse",
            deduplicate_by=("event_id",),
            error_type="validation",
        )
        inserted += load_dataframe(
            validation.valid_records, connection, TEMPORARY_TABLE_LOAD
        )
        validation_errors_quarantined |= validation.rejected_records_written
        validation.release()
        processed_batches += 1

    runtime.emit_summary(
        dynamic_frame_errors_quarantined=dynamic_frame_errors_quarantined,
        validation_errors_quarantined=validation_errors_quarantined,
        processed_batches=processed_batches,
        warehouse_rows_inserted=inserted,
        processing_mode=processing_mode.value,
        source_uri=job.source_uri,
    )
    job.commit()


if __name__ == "__main__":
    main()
