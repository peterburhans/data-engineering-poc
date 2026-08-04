# Mock data service

One extensible runtime for local data producers. It currently supports:

- continuous smart-meter readings published to Kinesis;
- effective-dated prices for all US states and Census regions published to a dedicated Kinesis stream;
- an internal pricing API at `GET /v1/prices/current`.

Providers implement the small `MockProvider` lifecycle and are registered in `providers/__init__.py`. Both live event types flow through Firehose, raw S3, curated Parquet, and Glue warehouse loaders. The service owns no database DDL.

Historical generation is a service CLI operation, not an Airflow DAG. It bypasses
Kinesis and Firehose while preserving the raw-zone contract:

```shell
python -m mock_data_service.main backfill --days 365
```

The integer is the number of days before the current UTC hour. Daily NDJSON objects are
written concurrently and idempotently to raw S3. Run the normal warehouse DAG afterward
to validate, quarantine, curate, load, and rebuild dbt models.
