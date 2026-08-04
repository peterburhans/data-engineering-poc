"""Discover lake partitions and dbt column lineage and publish them to OpenLineage."""

import json
import os
import re
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path

import boto3
import psycopg2
from airflow.sdk import dag, task
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import (
    DatasetEvent,
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    StaticDataset,
)
from openlineage.client.facet_v2 import column_lineage_dataset
from openlineage_sql import parse as parse_sql

PRODUCER = "https://github.com/openlineage/openlineage"
LINEAGE_NAMESPACE = "local-smart-meter"
POSTGRES_NAMESPACE = "postgres://mooncake:5432"
RAW_BUCKET = os.getenv("RAW_BUCKET", "local-smart-meter-raw-s3")
CURATED_BUCKET = os.getenv("CURATED_BUCKET", "local-smart-meter-curated-s3")
RAW_PARTITION = re.compile(
    r"^(meter-readings/year=\d{4}/month=\d{2}/day=\d{2}/hour=\d{2})/"
)
CURATED_PARTITION = re.compile(r"^(meter_readings/year=\d{4}/month=\d{2}/day=\d{2})/")
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_COMPILED_TARGET = "/opt/airflow/logs/lineage-dbt-target"


def lineage_client() -> OpenLineageClient:
    transport = json.loads(os.environ["AIRFLOW__OPENLINEAGE__TRANSPORT"])
    return OpenLineageClient(config={"transport": transport})


def event_time() -> str:
    return datetime.now(UTC).isoformat()


def emit_dataset(client: OpenLineageClient, namespace: str, name: str) -> None:
    client.emit(
        DatasetEvent(
            eventTime=event_time(),
            producer=PRODUCER,
            dataset=StaticDataset(namespace=namespace, name=name),
        )
    )


def s3_partitions(bucket: str, pattern: re.Pattern[str]) -> list[str]:
    s3 = boto3.client("s3", endpoint_url=os.getenv("AWS_ENDPOINT_URL"))
    partitions: set[str] = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for item in page.get("Contents", []):
            match = pattern.match(item["Key"])
            if match:
                partitions.add(match.group(1))
    return sorted(partitions)


def emit_job_lineage(
    job_name: str, inputs: list[InputDataset], outputs: list[OutputDataset]
) -> None:
    lineage_client().emit(
        RunEvent(
            eventTime=event_time(),
            producer=PRODUCER,
            eventType="COMPLETE",
            run=Run(runId=str(uuid.uuid4())),
            job=Job(namespace=LINEAGE_NAMESPACE, name=job_name),
            inputs=inputs,
            outputs=outputs,
        )
    )


def relation_name(database: str | None, schema: str | None, identifier: str) -> str:
    return ".".join(
        part for part in (database or "warehouse", schema, identifier) if part
    )


def warehouse_columns(relations: set[str]) -> dict[str, set[str]]:
    relation_parts = {
        relation: relation.split(".", 2)[1:]
        for relation in relations
        if relation.count(".") == 2
    }
    columns = {relation: set() for relation in relation_parts}
    with (
        psycopg2.connect(
            host="mooncake",
            port=5432,
            dbname="warehouse",
            user="mooncake",
            password=os.getenv("MOONCAKE_PASSWORD", "mooncake"),
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            """
                select table_schema, table_name, column_name
                from information_schema.columns
                where table_schema = any(%s)
                """,
            (sorted({parts[0] for parts in relation_parts.values()}),),
        )
        by_table: dict[tuple[str, str], set[str]] = {}
        for schema, table, column in cursor.fetchall():
            by_table.setdefault((schema, table), set()).add(column)
    for relation, parts in relation_parts.items():
        columns[relation] = by_table.get((parts[0], parts[1]), set())
    return columns


def column_lineage_facet(
    compiled_sql: str,
    fallback_inputs: set[str],
    output_relation: str,
    columns: dict[str, set[str]],
) -> tuple[object | None, set[str]]:
    parsed = parse_sql([compiled_sql], dialect="postgres", default_schema="public")
    fields: dict[str, column_lineage_dataset.Fields] = {}
    input_relations: set[str] = set()
    for item in parsed.column_lineage:
        input_fields = []
        for source in item.lineage:
            if source.origin is None:
                continue
            source_relation = relation_name(
                source.origin.database,
                source.origin.schema,
                source.origin.name,
            )
            input_relations.add(source_relation)
            input_fields.append(
                column_lineage_dataset.InputField(
                    namespace=POSTGRES_NAMESPACE,
                    name=source_relation,
                    field=source.name,
                )
            )
        if input_fields:
            fields[item.descendant.name] = column_lineage_dataset.Fields(
                inputFields=input_fields
            )
    input_relations.update(fallback_inputs)
    for output_column in columns.get(output_relation, set()) - fields.keys():
        input_fields = [
            column_lineage_dataset.InputField(
                namespace=POSTGRES_NAMESPACE,
                name=input_relation,
                field=output_column,
            )
            for input_relation in sorted(input_relations)
            if output_column in columns.get(input_relation, set())
        ]
        if input_fields:
            fields[output_column] = column_lineage_dataset.Fields(
                inputFields=input_fields
            )
    if not fields:
        return None, input_relations
    return column_lineage_dataset.ColumnLineageDatasetFacet(
        fields=fields
    ), input_relations


@dag(
    dag_id="smart_meter_lineage_catalog",
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["smart-meter", "openlineage", "s3", "dbt", "catalog"],
)
def smart_meter_lineage_catalog() -> None:
    @task
    def scan_raw_s3() -> list[str]:
        partitions = s3_partitions(RAW_BUCKET, RAW_PARTITION)
        client = lineage_client()
        emit_dataset(client, f"s3://{RAW_BUCKET}", "meter-readings")
        for partition in partitions:
            emit_dataset(client, f"s3://{RAW_BUCKET}", partition)
        return partitions

    @task
    def scan_curated_s3() -> list[str]:
        partitions = s3_partitions(CURATED_BUCKET, CURATED_PARTITION)
        client = lineage_client()
        emit_dataset(client, f"s3://{CURATED_BUCKET}", "meter_readings")
        for partition in partitions:
            emit_dataset(client, f"s3://{CURATED_BUCKET}", partition)
        return partitions

    @task
    def scan_dbt_manifest() -> int:
        subprocess.run(
            [
                "/home/airflow/dbt_venv/bin/dbt",
                "compile",
                "--project-dir",
                DBT_PROJECT_DIR,
                "--profiles-dir",
                DBT_PROJECT_DIR,
                "--target-path",
                DBT_COMPILED_TARGET,
            ],
            check=True,
        )
        manifest = json.loads(Path(DBT_COMPILED_TARGET, "manifest.json").read_text())
        client = lineage_client()
        relations: set[str] = set()
        for collection in (manifest.get("nodes", {}), manifest.get("sources", {})):
            for node in collection.values():
                if node.get("resource_type") not in {
                    "model",
                    "seed",
                    "snapshot",
                    "source",
                }:
                    continue
                database = node.get("database") or "warehouse"
                schema = node.get("schema")
                identifier = (
                    node.get("alias") or node.get("identifier") or node.get("name")
                )
                if schema and identifier:
                    relations.add(relation_name(database, schema, identifier))
        for relation in sorted(relations):
            emit_dataset(client, POSTGRES_NAMESPACE, relation)

        node_relations = {}
        for collection in (manifest.get("nodes", {}), manifest.get("sources", {})):
            for unique_id, node in collection.items():
                schema = node.get("schema")
                identifier = (
                    node.get("alias") or node.get("identifier") or node.get("name")
                )
                if schema and identifier:
                    node_relations[unique_id] = relation_name(
                        node.get("database") or "warehouse", schema, identifier
                    )
        columns = warehouse_columns(set(node_relations.values()))

        emitted_models = 0
        for node in manifest.get("nodes", {}).values():
            if node.get("resource_type") != "model" or not node.get("compiled_code"):
                continue
            database = node.get("database") or "warehouse"
            schema = node.get("schema")
            identifier = node.get("alias") or node.get("name")
            if not schema or not identifier:
                continue
            output_relation = relation_name(database, schema, identifier)
            fallback_inputs = {
                node_relations[parent]
                for parent in node.get("depends_on", {}).get("nodes", [])
                if parent in node_relations
            }
            facet, input_relations = column_lineage_facet(
                node["compiled_code"], fallback_inputs, output_relation, columns
            )
            if facet is None:
                continue
            emit_job_lineage(
                f"dbt.column_lineage.{node['name']}",
                [
                    InputDataset(namespace=POSTGRES_NAMESPACE, name=relation)
                    for relation in sorted(input_relations)
                ],
                [
                    OutputDataset(
                        namespace=POSTGRES_NAMESPACE,
                        name=output_relation,
                        facets={"columnLineage": facet},
                    )
                ],
            )
            emitted_models += 1
        if emitted_models == 0:
            raise RuntimeError("dbt compilation produced no column-lineage facets")
        return len(relations)

    @task
    def publish_warehouse_edges(
        raw_partitions: list[str], curated_partitions: list[str]
    ) -> None:
        raw_namespace = f"s3://{RAW_BUCKET}"
        curated_namespace = f"s3://{CURATED_BUCKET}"
        raw_inputs = [
            InputDataset(namespace=raw_namespace, name=name) for name in raw_partitions
        ]
        curated_inputs = [
            InputDataset(namespace=curated_namespace, name=name)
            for name in curated_partitions
        ]
        curated_outputs = [
            OutputDataset(namespace=curated_namespace, name=name)
            for name in curated_partitions
        ]
        emit_job_lineage(
            "smart_meter_warehouse.smart_meter_raw_to_curated",
            raw_inputs
            or [InputDataset(namespace=raw_namespace, name="meter-readings")],
            curated_outputs
            or [OutputDataset(namespace=curated_namespace, name="meter_readings")],
        )
        emit_job_lineage(
            "smart_meter_warehouse.curated_parquet_to_mooncake",
            curated_inputs
            or [InputDataset(namespace=curated_namespace, name="meter_readings")],
            [
                OutputDataset(
                    namespace=POSTGRES_NAMESPACE,
                    name="warehouse.core_raw.smart_meter_events",
                )
            ],
        )

    raw = scan_raw_s3()
    curated = scan_curated_s3()
    dbt_relations = scan_dbt_manifest()
    publish_warehouse_edges(raw, curated) >> dbt_relations


smart_meter_lineage_catalog()
