output "resource_names" {
  description = "Canonical physical names consumed by orchestration and smoke tests."
  value       = local.names
}

output "kinesis_stream_name" {
  description = "Kinesis stream receiving smart-meter events."
  value       = module.streaming.kinesis_stream_name
}

output "pricing_kinesis_stream_name" {
  description = "Kinesis stream receiving regional electricity price events."
  value       = module.price_streaming.kinesis_stream_name
}

output "raw_bucket_name" {
  description = "S3 raw-zone bucket name."
  value       = module.data_lake.buckets.raw.name
}

output "curated_bucket_name" {
  description = "S3 curated-zone bucket name."
  value       = module.data_lake.buckets.curated.name
}

output "bookmark_table_name" {
  description = "DynamoDB table used for local Glue image object bookmarks."
  value       = aws_dynamodb_table.bookmarks.name
}
