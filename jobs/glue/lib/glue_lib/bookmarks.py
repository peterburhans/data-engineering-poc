"""Object-level bookmarks for the local AWS Glue Docker runner."""

from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlparse

import boto3
from boto3.dynamodb.conditions import Key

from glue_lib.runtime import GlueRuntime


@dataclass
class LocalObjectBookmarks:
    """Track successfully processed S3 objects in DynamoDB by pipeline stage."""

    runtime: GlueRuntime
    table_name: str
    stage: str

    @classmethod
    def from_runtime(cls, runtime: GlueRuntime) -> "LocalObjectBookmarks | None":
        table_name = runtime.optional_argument("local-bookmark-table")
        stage = runtime.optional_argument("local-bookmark-stage")
        if table_name is None and stage is None:
            return None
        if not table_name or not stage:
            raise ValueError(
                "--local-bookmark-table and --local-bookmark-stage must be supplied together"
            )
        return cls(runtime, table_name, stage)

    @property
    def table(self):
        return boto3.resource("dynamodb", endpoint_url=self.runtime.endpoint_url).Table(
            self.table_name
        )

    def _processed_uris(self) -> set[str]:
        processed: set[str] = set()
        arguments = {
            "KeyConditionExpression": Key("stage").eq(self.stage),
            "ProjectionExpression": "object_uri",
        }
        while True:
            response = self.table.query(**arguments)
            processed.update(item["object_uri"] for item in response.get("Items", []))
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                return processed
            arguments["ExclusiveStartKey"] = last_key

    def unprocessed_s3_objects(self, source_uri: str, data_format: str) -> list[str]:
        parsed = urlparse(source_uri)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
        processed = self._processed_uris()
        objects: list[str] = []
        paginator = boto3.client(
            "s3", endpoint_url=self.runtime.endpoint_url
        ).get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item["Key"]
                if key.endswith("/"):
                    continue
                if data_format == "parquet" and not key.endswith(".parquet"):
                    continue
                object_uri = f"s3://{bucket}/{key}"
                if object_uri not in processed:
                    objects.append(object_uri)
        return sorted(objects)

    def commit(self, object_uris: list[str]) -> None:
        if not object_uris:
            return
        completed_at = datetime.now(UTC).isoformat()
        with self.table.batch_writer() as batch:
            for object_uri in object_uris:
                batch.put_item(
                    Item={
                        "stage": self.stage,
                        "object_uri": object_uri,
                        "completed_at": completed_at,
                        "job_run_id": self.runtime.run_id,
                    }
                )
