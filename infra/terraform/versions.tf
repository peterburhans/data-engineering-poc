terraform {
  required_version = ">= 1.9.0, < 2.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.100"
    }
  }
}

provider "aws" {
  region = var.aws_region

  skip_credentials_validation = var.aws_endpoint_url != null
  skip_metadata_api_check     = var.aws_endpoint_url != null
  skip_requesting_account_id  = var.aws_endpoint_url != null
  s3_use_path_style           = var.aws_endpoint_url != null

  endpoints {
    firehose       = var.aws_endpoint_url
    dynamodb       = var.aws_endpoint_url
    iam            = var.aws_endpoint_url
    kinesis        = var.aws_endpoint_url
    logs           = var.aws_endpoint_url
    s3             = var.aws_endpoint_url
    secretsmanager = var.aws_endpoint_url
    sts            = var.aws_endpoint_url
  }

  default_tags {
    tags = merge(local.required_tags, var.common_tags)
  }
}
