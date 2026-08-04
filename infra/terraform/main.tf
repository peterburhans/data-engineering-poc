locals {
  # AWS resources that permit underscores follow: {env}_{name}_{resource_shortcode}.
  name_prefix   = "${var.environment}_${var.name}"
  bucket_prefix = replace(local.name_prefix, "_", "-")

  names = {
    raw_bucket            = "${local.bucket_prefix}-raw-s3"
    curated_bucket        = "${local.bucket_prefix}-curated-s3"
    event_stream          = "${local.name_prefix}_events_kds"
    raw_delivery_stream   = "${local.name_prefix}_raw_fh"
    firehose_role         = "${local.name_prefix}_firehose_iamr"
    firehose_policy       = "${local.name_prefix}_firehose_iamp"
    price_stream          = "${local.name_prefix}_prices_kds"
    price_delivery        = "${local.name_prefix}_prices_fh"
    price_firehose_role   = "${local.name_prefix}_prices_firehose_iamr"
    price_firehose_policy = "${local.name_prefix}_prices_firehose_iamp"
    warehouse_secret      = "${local.name_prefix}_warehouse_sm"
    bookmark_table        = "${local.name_prefix}_bookmarks_ddb"
  }

  required_tags = {
    Environment = var.environment
    ManagedBy   = "terraform"
    Project     = var.name
  }
}

module "data_lake" {
  source = "./modules/data_lake"

  bucket_names = {
    raw     = local.names.raw_bucket
    curated = local.names.curated_bucket
  }
  force_destroy = var.force_destroy_buckets
}

module "streaming" {
  source = "./modules/streaming"

  names = {
    event_stream    = local.names.event_stream
    delivery_stream = local.names.raw_delivery_stream
    firehose_role   = local.names.firehose_role
    firehose_policy = local.names.firehose_policy
  }
  raw_bucket       = module.data_lake.buckets.raw
  raw_prefix       = "meter-readings"
  event_time_field = "event_time"
  buffering = var.environment == "local" ? {
    interval_seconds = 5
    size_mb          = 1
    } : {
    interval_seconds = 60
    size_mb          = 64
  }
}

module "price_streaming" {
  source = "./modules/streaming"

  names = {
    event_stream    = local.names.price_stream
    delivery_stream = local.names.price_delivery
    firehose_role   = local.names.price_firehose_role
    firehose_policy = local.names.price_firehose_policy
  }
  raw_bucket       = module.data_lake.buckets.raw
  raw_prefix       = "electricity-prices"
  event_time_field = "effective_from"
  buffering = var.environment == "local" ? {
    interval_seconds = 5
    size_mb          = 1
    } : {
    interval_seconds = 60
    size_mb          = 64
  }
}

resource "aws_secretsmanager_secret" "warehouse" {
  name        = local.names.warehouse_secret
  description = "Warehouse connection used by the local Glue container runner."
}

resource "aws_secretsmanager_secret_version" "warehouse" {
  secret_id = aws_secretsmanager_secret.warehouse.id
  secret_string = jsonencode(merge(var.warehouse_connection, {
    password = var.mooncake_password
  }))
}

resource "aws_dynamodb_table" "bookmarks" {
  name         = local.names.bookmark_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "stage"
  range_key    = "object_uri"

  attribute {
    name = "stage"
    type = "S"
  }

  attribute {
    name = "object_uri"
    type = "S"
  }
}
