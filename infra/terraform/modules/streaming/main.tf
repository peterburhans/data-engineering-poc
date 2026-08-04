resource "aws_kinesis_stream" "events" {
  name             = var.names.event_stream
  shard_count      = 1
  retention_period = 24

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

data "aws_iam_policy_document" "firehose_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = var.names.firehose_role
  assume_role_policy = data.aws_iam_policy_document.firehose_assume_role.json
}

data "aws_iam_policy_document" "firehose" {
  statement {
    sid = "WriteRawObjects"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:PutObject",
    ]
    resources = [var.raw_bucket.arn, "${var.raw_bucket.arn}/*"]
  }

  statement {
    sid = "ReadKinesisSource"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards",
    ]
    resources = [aws_kinesis_stream.events.arn]
  }
}

resource "aws_iam_role_policy" "firehose" {
  name   = var.names.firehose_policy
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose.json
}

resource "aws_kinesis_firehose_delivery_stream" "raw" {
  name        = var.names.delivery_stream
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.events.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = var.raw_bucket.arn
    buffering_interval  = var.buffering.interval_seconds
    buffering_size      = var.buffering.size_mb
    prefix              = "${var.raw_prefix}/year=!{partitionKeyFromQuery:year}/month=!{partitionKeyFromQuery:month}/day=!{partitionKeyFromQuery:day}/hour=!{partitionKeyFromQuery:hour}/"
    error_output_prefix = "errors/source=firehose/stage=${var.raw_prefix}/error_type=!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"

    dynamic_partitioning_configuration {
      enabled = true
    }

    processing_configuration {
      enabled = true
      processors {
        type = "MetadataExtraction"
        parameters {
          parameter_name  = "MetadataExtractionQuery"
          parameter_value = "{year:.${var.event_time_field}[0:4],month:.${var.event_time_field}[5:7],day:.${var.event_time_field}[8:10],hour:.${var.event_time_field}[11:13]}"
        }
        parameters {
          parameter_name  = "JsonParsingEngine"
          parameter_value = "JQ-1.6"
        }
      }
    }
  }

  depends_on = [aws_iam_role_policy.firehose]
}
