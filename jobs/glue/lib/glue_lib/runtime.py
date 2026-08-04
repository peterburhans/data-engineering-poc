"""Glue runtime initialization, local endpoint configuration, and observability."""

import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from pyspark.sql import SparkSession


@dataclass(frozen=True)
class GlueRuntime:
    """Initialized AWS Glue runtime shared by all pipeline jobs."""

    args: dict[str, str]
    glue_context: GlueContext
    spark: SparkSession
    job: Job
    endpoint_url: str | None
    run_id: str
    started_at: datetime
    managed_glue_job: bool

    @classmethod
    def create(cls, required_arguments: list[str]) -> "GlueRuntime":
        started_at = datetime.now(UTC)
        args = getResolvedOptions(sys.argv, ["JOB_NAME", *required_arguments])
        glue_context = GlueContext(SparkContext.getOrCreate())
        endpoint_url = cls.optional_argument("aws_endpoint_url")
        cls.configure_localstack(glue_context, endpoint_url)
        spark = glue_context.spark_session
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        job = Job(glue_context)
        managed_glue_job = cls.optional_argument("local-bookmark-table") is None
        if managed_glue_job:
            job.init(args["JOB_NAME"], args)
        run_id = cls.optional_argument("JOB_RUN_ID") or "local"
        return cls(
            args,
            glue_context,
            spark,
            job,
            endpoint_url,
            run_id,
            started_at,
            managed_glue_job,
        )

    @staticmethod
    def optional_argument(name: str) -> str | None:
        flag = f"--{name}"
        if flag not in sys.argv:
            return None
        index = sys.argv.index(flag)
        if index + 1 >= len(sys.argv):
            raise ValueError(f"Glue argument {flag} requires a value")
        return sys.argv[index + 1]

    @staticmethod
    def configure_localstack(
        glue_context: GlueContext, endpoint_url: str | None
    ) -> None:
        if endpoint_url is None:
            return
        configuration = (
            glue_context.spark_session.sparkContext._jsc.hadoopConfiguration()
        )
        configuration.set("fs.s3a.endpoint", endpoint_url)
        configuration.set("fs.s3a.path.style.access", "true")
        configuration.set("fs.s3a.connection.ssl.enabled", "false")
        configuration.set(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )

    def read_json_secret(
        self, secret_id: str, required_fields: set[str]
    ) -> dict[str, Any]:
        response = boto3.client(
            "secretsmanager", endpoint_url=self.endpoint_url
        ).get_secret_value(SecretId=secret_id)
        secret = json.loads(response["SecretString"])
        missing = sorted(required_fields - secret.keys())
        if missing:
            raise ValueError(
                f"Secret {secret_id!r} is missing fields: {', '.join(missing)}"
            )
        return secret

    def emit_summary(self, **values: Any) -> None:
        now = datetime.now(UTC)
        self.glue_context.get_logger().info(
            json.dumps(
                {
                    "event": "glue_job_summary",
                    "job": self.args["JOB_NAME"],
                    "run_id": self.run_id,
                    "timestamp": now.isoformat(),
                    "duration_seconds": round(
                        (now - self.started_at).total_seconds(), 3
                    ),
                    **values,
                },
                default=str,
                sort_keys=True,
            )
        )

    def commit(self) -> None:
        if self.managed_glue_job:
            self.job.commit()
