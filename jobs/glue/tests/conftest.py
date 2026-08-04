"""Minimal AWS Glue and Spark module doubles for shared-library unit tests."""

import os
import sys
from types import ModuleType


def _module(name: str) -> ModuleType:
    module = ModuleType(name)
    sys.modules[name] = module
    return module


awsglue = _module("awsglue")
awsglue.__path__ = []

awsglue_context = _module("awsglue.context")
awsglue_context.GlueContext = type("GlueContext", (), {})

awsglue_dynamicframe = _module("awsglue.dynamicframe")
awsglue_dynamicframe.DynamicFrame = type("DynamicFrame", (), {})

awsglue_job = _module("awsglue.job")
awsglue_job.Job = type("Job", (), {})

awsglue_utils = _module("awsglue.utils")
awsglue_utils.getResolvedOptions = lambda *_args, **_kwargs: {}

if os.environ.get("GLUE_SPARK_TESTS") != "1":
    pyspark = _module("pyspark")
    pyspark.__path__ = []
    pyspark.SparkContext = type("SparkContext", (), {})

    pyspark_sql = _module("pyspark.sql")
    pyspark_sql.__path__ = []
    for class_name in ("Column", "DataFrame", "Observation", "SparkSession"):
        setattr(pyspark_sql, class_name, type(class_name, (), {}))

    pyspark_functions = _module("pyspark.sql.functions")
    pyspark_sql.functions = pyspark_functions
