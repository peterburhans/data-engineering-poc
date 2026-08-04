from __future__ import annotations

from datetime import UTC, datetime

from airflow.sdk import dag
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior
from glue_tasks import (
    BOOKMARK_TABLE,
    CURATED_BUCKET,
    RAW_BUCKET,
    WAREHOUSE_SECRET,
    glue_operator,
)

PROCESSING_MODE = "{{ dag_run.conf.get('processing_mode', 'incremental') }}"
BACKFILL_ARGUMENTS = {
    "backfill_start": "{{ dag_run.conf.get('backfill_start', '') }}",
    "backfill_end": "{{ dag_run.conf.get('backfill_end', '') }}",
    "backfill_grain": "{{ dag_run.conf.get('backfill_grain', 'day') }}",
}
BOOKMARK_OPTION = (
    "{{ 'job-bookmark-disable' if dag_run.conf.get('processing_mode') == 'backfill' "
    "else 'job-bookmark-enable' }}"
)


@dag(
    dag_id="smart_meter_warehouse",
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["smart-meter", "glue", "parquet", "data-vault", "dbt"],
    doc_md="""
    ## Reprocessing the lake

    Use **Trigger → Single Run** with this Configuration JSON:

    ```json
    {
      "processing_mode": "backfill",
      "backfill_start": "2025-08-04T00:00:00Z",
      "backfill_end": "2026-08-04T00:00:00Z",
      "backfill_grain": "day"
    }
    ```

    This performs one complete, idempotent raw → curated → warehouse replay with
    bookmarks disabled. Do not use Airflow's date-range **Backfill** mode for a lake
    replay: this DAG is scheduled every 15 minutes and each generated logical run
    would repeat the same S3-wide replay.
    """,
)
def smart_meter_warehouse() -> None:
    smart_meter_raw_to_curated = glue_operator(
        "smart_meter_raw_to_curated",
        "smart_meter_raw_to_curated.py",
        {
            "JOB_NAME": "local_smart_meter_raw_to_curated",
            "aws_endpoint_url": "http://localstack:4566",
            "raw_bucket": RAW_BUCKET,
            "raw_prefix": "meter-readings/",
            "curated_bucket": CURATED_BUCKET,
            "curated_prefix": "meter_readings/",
            "error_bucket": RAW_BUCKET,
            "error_prefix": "errors/source=firehose/stage=smart_meter_raw_to_curated/",
            "processing_mode": PROCESSING_MODE,
            **BACKFILL_ARGUMENTS,
            "job-bookmark-option": BOOKMARK_OPTION,
            "local-bookmark-table": BOOKMARK_TABLE,
            "local-bookmark-stage": "smart_meter_raw_to_curated",
        },
    )

    smart_meter_curated_to_warehouse = glue_operator(
        "smart_meter_curated_to_warehouse",
        "smart_meter_curated_to_warehouse.py",
        {
            "JOB_NAME": "local_smart_meter_curated_to_warehouse",
            "aws_endpoint_url": "http://localstack:4566",
            "curated_bucket": CURATED_BUCKET,
            "curated_prefix": "meter_readings/",
            "error_bucket": RAW_BUCKET,
            "error_prefix": "errors/source=curated/stage=smart_meter_curated_to_warehouse/",
            "warehouse_secret_id": WAREHOUSE_SECRET,
            "warehouse_ssl_enabled": "false",
            "warehouse_connect_timeout": "15",
            "warehouse_statement_timeout": "300",
            "processing_mode": PROCESSING_MODE,
            **BACKFILL_ARGUMENTS,
            "job-bookmark-option": BOOKMARK_OPTION,
            "local-bookmark-table": BOOKMARK_TABLE,
            "local-bookmark-stage": "smart_meter_curated_to_warehouse",
        },
    )

    prices_raw_to_curated = glue_operator(
        "prices_raw_to_curated",
        "prices_raw_to_curated.py",
        {
            "JOB_NAME": "local_smart_meter_prices_raw_to_curated",
            "aws_endpoint_url": "http://localstack:4566",
            "raw_bucket": RAW_BUCKET,
            "raw_prefix": "electricity-prices/",
            "curated_bucket": CURATED_BUCKET,
            "curated_prefix": "electricity_prices/",
            "error_bucket": RAW_BUCKET,
            "error_prefix": "errors/source=firehose/stage=prices_raw_to_curated/",
            "processing_mode": PROCESSING_MODE,
            **BACKFILL_ARGUMENTS,
            "job-bookmark-option": BOOKMARK_OPTION,
            "local-bookmark-table": BOOKMARK_TABLE,
            "local-bookmark-stage": "prices_raw_to_curated",
        },
    )

    prices_curated_to_warehouse = glue_operator(
        "prices_curated_to_warehouse",
        "prices_curated_to_warehouse.py",
        {
            "JOB_NAME": "local_smart_meter_prices_curated_to_warehouse",
            "aws_endpoint_url": "http://localstack:4566",
            "curated_bucket": CURATED_BUCKET,
            "curated_prefix": "electricity_prices/",
            "error_bucket": RAW_BUCKET,
            "error_prefix": "errors/source=curated/stage=prices_curated_to_warehouse/",
            "warehouse_secret_id": WAREHOUSE_SECRET,
            "warehouse_ssl_enabled": "false",
            "warehouse_connect_timeout": "15",
            "warehouse_statement_timeout": "300",
            "processing_mode": PROCESSING_MODE,
            **BACKFILL_ARGUMENTS,
            "job-bookmark-option": BOOKMARK_OPTION,
            "local-bookmark-table": BOOKMARK_TABLE,
            "local-bookmark-stage": "prices_curated_to_warehouse",
        },
    )

    dbt_models = DbtTaskGroup(
        group_id="dbt_warehouse",
        project_config=ProjectConfig(
            "/opt/airflow/dbt",
            manifest_path="/opt/airflow/dbt/target/manifest.json",
            install_dbt_deps=False,
        ),
        profile_config=ProfileConfig(
            profile_name="smart_meter",
            target_name="local",
            profiles_yml_filepath="/opt/airflow/dbt/profiles.yml",
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            dbt_executable_path="/home/airflow/dbt_venv/bin/dbt",
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            test_behavior=TestBehavior.AFTER_ALL,
        ),
        operator_args={"install_deps": False},
    )

    smart_meter_raw_to_curated >> smart_meter_curated_to_warehouse
    prices_raw_to_curated >> prices_curated_to_warehouse
    [smart_meter_curated_to_warehouse, prices_curated_to_warehouse] >> dbt_models


smart_meter_warehouse()
