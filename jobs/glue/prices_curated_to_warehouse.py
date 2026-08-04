"""Merge curated regional prices into Mooncake."""

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

MAPPINGS = [
    ColumnMapping("price_id", "price_id", "string"),
    ColumnMapping("schema_version", "schema_version", "string"),
    ColumnMapping("us_region", "us_region", "string"),
    ColumnMapping("state_code", "state_code", "string"),
    ColumnMapping("currency_code", "currency_code", "string"),
    ColumnMapping("effective_from", "effective_from", "timestamp"),
    ColumnMapping("price_per_kwh", "price_per_kwh", "double"),
    ColumnMapping("source_system", "source_system", "string"),
]
COLUMNS = tuple(mapping.target for mapping in MAPPINGS)
VALIDATION_RULES = [
    ValidationRule(
        "format:price_id_uuid", lambda: F.col("price_id").rlike(UUID_PATTERN)
    ),
    ValidationRule(
        "schema:supported_version",
        lambda: F.col("schema_version").isin(*SUPPORTED_SCHEMA_VERSIONS),
    ),
    ValidationRule(
        "region:us_census", lambda: F.col("us_region").isin(*US_CENSUS_REGIONS)
    ),
    ValidationRule(
        "state:supported_code", lambda: F.col("state_code").isin(*US_STATE_CODES)
    ),
    ValidationRule(
        "region:state_alignment",
        lambda: F.concat_ws(":", "state_code", "us_region").isin(*US_STATE_REGION_KEYS),
    ),
    ValidationRule("range:price", lambda: F.col("price_per_kwh") >= 0),
]
LOAD = TemporaryTableLoad(
    create_sql="""create temporary table ingest_prices (
        price_id text, schema_version text, us_region text, state_code text,
        currency_code text, effective_from timestamptz, price_per_kwh numeric,
        source_system text) on commit drop""",
    insert_sql=f"insert into ingest_prices ({', '.join(COLUMNS)}) values ({', '.join(['%s'] * len(COLUMNS))})",
    merge_sql="""insert into core_raw.electricity_prices (
        price_id, schema_version, us_region, state_code, currency_code,
        effective_from, price_per_kwh, source_system)
        select price_id::uuid, schema_version, us_region, state_code,
        currency_code, effective_from, price_per_kwh, source_system from ingest_prices
        on conflict (state_code, effective_from) do update set
        price_per_kwh = excluded.price_per_kwh, source_system = excluded.source_system,
        load_datetime = now()""",
    columns=COLUMNS,
)


def main() -> None:
    job = GlueJob(
        required_arguments=[
            "warehouse_secret_id",
            "warehouse_ssl_enabled",
            "warehouse_connect_timeout",
            "warehouse_statement_timeout",
            "processing_mode",
            *BACKFILL_ARGUMENTS,
        ],
        mappings=MAPPINGS,
        required_columns=list(COLUMNS),
        validation_rules=VALIDATION_RULES,
        source=S3LocationArguments("curated_bucket", "curated_prefix"),
        errors=S3LocationArguments("error_bucket", "error_prefix"),
    )
    processing_mode = ProcessingMode.from_runtime(job.runtime)
    backfill_window = BackfillWindow.from_runtime(job.runtime, processing_mode)
    secret = job.runtime.read_json_secret(
        job.argument("warehouse_secret_id"),
        {"host", "port", "database", "username", "password"},
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
    for batch_number, window in enumerate(windows, start=1):
        frame = job.read_s3(
            uri=job.source_uri,
            data_format="parquet",
            transformation_context=f"curated_prices_batch_{batch_number}",
            backfill_window=window,
        )
        if frame is None:
            continue
        records = frame.toDF()
        if window is not None:
            records = window.filter(records, "effective_from")
        validation = job.validate(
            records,
            error_stage="prices_curated_to_warehouse",
            deduplicate_by=("price_id",),
        )
        inserted += load_dataframe(validation.valid_records, connection, LOAD)
        validation.release()
        processed_batches += 1
    job.runtime.emit_summary(
        processed_batches=processed_batches,
        warehouse_rows_inserted=inserted,
        processing_mode=processing_mode.value,
        source_uri=job.source_uri,
    )
    job.commit()


if __name__ == "__main__":
    main()
