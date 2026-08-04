output "kinesis_stream_name" {
  description = "Kinesis stream receiving smart-meter events."
  value       = aws_kinesis_stream.events.name
}

output "firehose_delivery_stream_name" {
  description = "Firehose delivery stream writing the raw zone."
  value       = aws_kinesis_firehose_delivery_stream.raw.name
}
