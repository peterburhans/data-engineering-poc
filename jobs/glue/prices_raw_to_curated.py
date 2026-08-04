"""Validate regional electricity price JSON and append curated Parquet."""

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


def main() -> None:
    job = GlueJob(
        required_arguments=["processing_mode", *BACKFILL_ARGUMENTS],
        mappings=MAPPINGS,
        required_columns=[mapping.target for mapping in MAPPINGS],
        validation_rules=[
            ValidationRule(
                "format:price_id_uuid", lambda: F.col("price_id").rlike(UUID_PATTERN)
            ),
            ValidationRule(
                "schema:supported_version",
                lambda: F.col("schema_version").isin(*SUPPORTED_SCHEMA_VERSIONS),
            ),
            ValidationRule(
                "region:us_census",
                lambda: F.col("us_region").isin(*US_CENSUS_REGIONS),
            ),
            ValidationRule(
                "state:supported_code",
                lambda: F.col("state_code").isin(*US_STATE_CODES),
            ),
            ValidationRule(
                "region:state_alignment",
                lambda: F.concat_ws(":", "state_code", "us_region").isin(
                    *US_STATE_REGION_KEYS
                ),
            ),
            ValidationRule("range:price", lambda: F.col("price_per_kwh") >= 0),
        ],
        source=S3LocationArguments("raw_bucket", "raw_prefix"),
        target=S3LocationArguments("curated_bucket", "curated_prefix"),
        errors=S3LocationArguments("error_bucket", "error_prefix"),
    )
    processing_mode = ProcessingMode.from_runtime(job.runtime)
    backfill_window = BackfillWindow.from_runtime(job.runtime, processing_mode)
    windows = backfill_window.batch_windows() if backfill_window else (None,)
    processed_batches = 0
    for batch_number, window in enumerate(windows, start=1):
        context_suffix = f"batch_{batch_number}"
        raw = job.read_s3(
            uri=job.source_uri,
            data_format="json",
            format_options={"multiline": False},
            transformation_context=f"raw_prices_{context_suffix}",
            backfill_window=window,
        )
        if raw is None:
            continue
        job.quarantine_dynamic_frame_errors(
            raw, transformation_context=f"write_price_frame_errors_{context_suffix}"
        )
        raw_records = raw.toDF()
        if window is not None:
            raw_records = window.filter(raw_records, "effective_from")
        validation = job.validate(
            raw_records,
            error_stage="prices_raw_to_curated",
            deduplicate_by=("price_id",),
        )
        curated = validation.valid_records.select(
            "*",
            F.date_format("effective_from", "yyyy").alias("year"),
            F.date_format("effective_from", "MM").alias("month"),
            F.date_format("effective_from", "dd").alias("day"),
        )
        write_partitioned_parquet(
            curated,
            job.target_uri,
            processing_mode,
            ("year", "month", "day"),
        )
        validation.release()
        processed_batches += 1
    job.runtime.emit_summary(
        processed_batches=processed_batches,
        processing_mode=processing_mode.value,
        source_uri=job.source_uri,
        target_uri=job.target_uri,
    )
    job.commit()


if __name__ == "__main__":
    main()
