"""Shared configuration and Docker operators for local Glue 5 jobs."""

import os
from datetime import timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

GLUE_IMAGE = os.getenv("GLUE_RUNNER_IMAGE", "local-smart-meter-glue:5")
PROJECT_ROOT = os.getenv("HOST_PROJECT_ROOT", "/workspace")
NETWORK_NAME = os.getenv("COMPOSE_NETWORK_NAME", "local_smart_meter_net")
RAW_BUCKET = os.getenv("RAW_BUCKET", "local-smart-meter-raw-s3")
CURATED_BUCKET = os.getenv("CURATED_BUCKET", "local-smart-meter-curated-s3")
BOOKMARK_TABLE = os.getenv("LOCAL_BOOKMARK_TABLE", "local_smart_meter_bookmarks_ddb")
WAREHOUSE_SECRET = os.getenv("WAREHOUSE_SECRET_ID", "local_smart_meter_warehouse_sm")
WORKSPACE = "/home/hadoop/workspace"
GLUE_PYTHONPATH = (
    "/usr/share/aws/glue-pds/PyGlue.zip:/usr/lib/spark/python/lib:"
    f"{WORKSPACE}/jobs/glue/lib:{WORKSPACE}/libs/data_contracts"
)
GLUE_LOCAL_PARALLELISM = os.getenv("GLUE_LOCAL_PARALLELISM", "4")


def glue_command(script: str, arguments: dict[str, str]) -> list[str]:
    command = [
        "spark-submit",
        "--conf",
        f"spark.executorEnv.PYTHONPATH={GLUE_PYTHONPATH}",
        "--conf",
        "spark.sql.adaptive.enabled=true",
        "--conf",
        "spark.sql.adaptive.coalescePartitions.enabled=true",
        "--conf",
        f"spark.sql.shuffle.partitions={GLUE_LOCAL_PARALLELISM}",
        "--conf",
        f"spark.default.parallelism={GLUE_LOCAL_PARALLELISM}",
        "--conf",
        "spark.sql.files.maxPartitionBytes=134217728",
        f"{WORKSPACE}/jobs/glue/{script}",
    ]
    for name, value in arguments.items():
        command.extend((f"--{name}", value))
    return command


def glue_operator(
    task_id: str, script: str, arguments: dict[str, str]
) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=GLUE_IMAGE,
        command=glue_command(script, arguments),
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK_NAME,
        mounts=[
            Mount(source=PROJECT_ROOT, target=WORKSPACE, type="bind", read_only=True)
        ],
        environment={
            "AWS_ACCESS_KEY_ID": "test",
            "AWS_SECRET_ACCESS_KEY": "test",
            "AWS_DEFAULT_REGION": "us-east-1",
            "PYTHONPATH": GLUE_PYTHONPATH,
        },
        auto_remove="success",
        mount_tmp_dir=False,
        retries=2,
        retry_delay=timedelta(seconds=30),
    )
