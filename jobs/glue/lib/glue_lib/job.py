"""Declarative base for AWS Glue jobs that map, validate, and split records."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import reduce
from operator import and_

from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from glue_lib.bookmarks import LocalObjectBookmarks
from glue_lib.processing import BackfillWindow, ProcessingMode
from glue_lib.runtime import GlueRuntime


@dataclass(frozen=True)
class ColumnMapping:
    """Map a source field to a typed target field."""

    source: str
    target: str
    data_type: str


@dataclass(frozen=True)
class ValidationRule:
    """A named predicate that returns true for valid records."""

    name: str
    predicate: Callable[[], Column]


@dataclass(frozen=True)
class S3LocationArguments:
    """Glue argument names that identify an S3 location."""

    bucket: str
    prefix: str


@dataclass
class ValidationOutcome:
    """Usable records and the cached mapped frame shared by both sinks."""

    valid_records: DataFrame
    rejected_records_written: bool
    _cached_records: DataFrame

    def release(self) -> None:
        """Release records cached for the valid and rejected consumers."""

        self._cached_records.unpersist()


class GlueJob:
    """Shared Glue job base for declarative mapping and validation."""

    def __init__(
        self,
        *,
        required_arguments: Sequence[str],
        mappings: Sequence[ColumnMapping],
        required_columns: Sequence[str],
        source: S3LocationArguments,
        errors: S3LocationArguments,
        target: S3LocationArguments | None = None,
        validation_rules: Sequence[ValidationRule] = (),
    ) -> None:
        location_arguments = [
            source.bucket,
            source.prefix,
            errors.bucket,
            errors.prefix,
        ]
        if target is not None:
            location_arguments.extend((target.bucket, target.prefix))
        arguments = list(dict.fromkeys([*required_arguments, *location_arguments]))
        self.runtime = GlueRuntime.create(arguments)
        self.mappings = tuple(mappings)
        self.required_columns = tuple(required_columns)
        self.validation_rules = tuple(validation_rules)
        self.source = source
        self.target = target
        self.errors = errors
        self.local_bookmarks = LocalObjectBookmarks.from_runtime(self.runtime)
        self._pending_source_uris: list[str] = []
        self._validate_definition()

    @property
    def args(self) -> dict[str, str]:
        return self.runtime.args

    def argument(self, name: str) -> str:
        """Return a required job argument with a useful configuration error."""

        try:
            return self.args[name]
        except KeyError as error:
            raise ValueError(f"Glue job argument {name!r} is not configured") from error

    @staticmethod
    def _s3_uri(bucket: str, prefix: str) -> str:
        normalized_bucket = bucket.removeprefix("s3://").strip("/")
        normalized_prefix = prefix.strip("/")
        if not normalized_bucket:
            raise ValueError("S3 bucket must not be empty")
        if not normalized_prefix:
            return f"s3://{normalized_bucket}/"
        return f"s3://{normalized_bucket}/{normalized_prefix}/"

    def _location_uri(self, location: S3LocationArguments) -> str:
        return self._s3_uri(
            self.argument(location.bucket), self.argument(location.prefix)
        )

    @property
    def source_uri(self) -> str:
        return self._location_uri(self.source)

    @property
    def target_uri(self) -> str:
        if self.target is None:
            raise ValueError("This Glue job does not define an S3 target")
        return self._location_uri(self.target)

    @property
    def error_uri(self) -> str:
        return self._location_uri(self.errors)

    def _validate_definition(self) -> None:
        targets = [mapping.target for mapping in self.mappings]
        duplicate_targets = sorted(
            target for target in set(targets) if targets.count(target) > 1
        )
        if duplicate_targets:
            raise ValueError(
                f"Mappings contain duplicate targets: {', '.join(duplicate_targets)}"
            )
        unknown_required = sorted(set(self.required_columns) - set(targets))
        if unknown_required:
            raise ValueError(
                "Required columns are not mapping targets: "
                f"{', '.join(unknown_required)}"
            )
        rule_names = [rule.name for rule in self.validation_rules]
        if len(rule_names) != len(set(rule_names)):
            raise ValueError("Validation rule names must be unique")

    def read_s3(
        self,
        *,
        uri: str,
        data_format: str,
        transformation_context: str,
        format_options: Mapping[str, object] | None = None,
        backfill_window: BackfillWindow | None = None,
    ) -> DynamicFrame | None:
        paths = backfill_window.source_paths(uri) if backfill_window else [uri]
        if (
            self.local_bookmarks is not None
            and self.argument("processing_mode") == ProcessingMode.INCREMENTAL.value
        ):
            paths = self.local_bookmarks.unprocessed_s3_objects(uri, data_format)
            self._pending_source_uris = paths
            if not paths:
                return None
        return self.runtime.glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": paths, "recurse": True},
            format=data_format,
            format_options=dict(format_options or {}),
            transformation_ctx=transformation_context,
        )

    def commit(self) -> None:
        """Commit local object bookmarks only after all job outputs succeed."""

        if self.local_bookmarks is not None:
            self.local_bookmarks.commit(self._pending_source_uris)
        self.runtime.commit()

    @staticmethod
    def quarantine_uri(error_uri: str, error_type: str) -> str:
        now = datetime.now(UTC)
        return (
            f"{error_uri}error_type={error_type}/year={now:%Y}/month={now:%m}/"
            f"day={now:%d}/hour={now:%H}/"
        )

    def quarantine_dynamic_frame_errors(
        self,
        frame: DynamicFrame,
        *,
        transformation_context: str,
    ) -> bool:
        """Write DynamicFrame conversion errors only when at least one exists."""

        errors = frame.errorsAsDynamicFrame().toDF()
        if errors.isEmpty():
            return False
        self.runtime.glue_context.write_dynamic_frame.from_options(
            frame=DynamicFrame.fromDF(
                errors,
                self.runtime.glue_context,
                transformation_context,
            ),
            connection_type="s3",
            connection_options={
                "path": self.quarantine_uri(self.error_uri, "dynamic_frame")
            },
            format="json",
            transformation_ctx=transformation_context,
        )
        return True

    def _write_rejected_records(
        self, records: DataFrame, *, error_type: str
    ) -> bool:
        """Write rejected records without creating empty quarantine objects."""

        if records.isEmpty():
            return False
        records.write.mode("append").json(
            self.quarantine_uri(self.error_uri, error_type)
        )
        return True

    def validate(
        self,
        frame: DataFrame,
        *,
        error_stage: str,
        additional_columns: Mapping[str, Column] | None = None,
        deduplicate_by: Sequence[str] = (),
        error_type: str = "quality",
    ) -> ValidationOutcome:
        """Map records, quarantine validation failures, and return valid records."""

        source = frame
        for mapping in self.mappings:
            if mapping.source not in source.columns:
                source = source.withColumn(mapping.source, F.lit(None))

        selections = [
            F.col(mapping.source).cast(mapping.data_type).alias(mapping.target)
            for mapping in self.mappings
        ]
        selections.extend(
            expression.alias(name)
            for name, expression in (additional_columns or {}).items()
        )
        # Both the valid sink and quarantine sink consume this frame. Cache it once
        # so validation and casting are not recomputed for each output action.
        mapped = source.select(*selections).persist()

        rules = [
            ValidationRule(
                f"required:{column}",
                lambda column=column: F.col(column).isNotNull(),
            )
            for column in self.required_columns
        ]
        rules.extend(self.validation_rules)
        evaluated = [
            (rule.name, F.coalesce(rule.predicate(), F.lit(False))) for rule in rules
        ]
        valid_condition = reduce(
            and_, (condition for _, condition in evaluated), F.lit(True)
        )
        error_reasons = F.array_compact(
            F.array(*[F.when(~condition, F.lit(name)) for name, condition in evaluated])
        )

        valid_records = mapped.filter(valid_condition)
        if deduplicate_by:
            valid_records = valid_records.dropDuplicates(list(deduplicate_by))
        rejected_records = mapped.filter(~valid_condition).select(
            "*",
            error_reasons.alias("error_reasons"),
            F.lit(error_type).alias("error_type"),
            F.lit(error_stage).alias("error_stage"),
            F.current_timestamp().alias("error_timestamp"),
        )
        rejected_records_written = self._write_rejected_records(
            rejected_records,
            error_type=error_type,
        )
        return ValidationOutcome(
            valid_records=valid_records,
            rejected_records_written=rejected_records_written,
            _cached_records=mapped,
        )
