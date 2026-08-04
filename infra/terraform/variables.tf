variable "aws_region" {
  description = "AWS region used by both AWS and LocalStack deployments."
  type        = string
  default     = "us-east-1"
}

variable "aws_endpoint_url" {
  description = "LocalStack edge endpoint. Leave null when deploying to AWS."
  type        = string
  default     = null
  nullable    = true
}

variable "environment" {
  description = "Lowercase deployment environment included in every physical resource name."
  type        = string
  default     = "local"

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{1,15}$", var.environment))
    error_message = "environment must be 2-16 lowercase alphanumeric/underscore characters."
  }
}

variable "name" {
  description = "Lowercase workload name included in every physical resource name."
  type        = string
  default     = "smart_meter"

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{1,31}$", var.name))
    error_message = "name must be 2-32 lowercase alphanumeric/underscore characters."
  }
}

variable "mooncake_password" {
  description = "Mooncake password stored in Secrets Manager for the curated loader."
  type        = string
  sensitive   = true
}

variable "warehouse_connection" {
  description = "Non-secret connection settings stored with the warehouse password in Secrets Manager."
  type = object({
    host     = string
    port     = number
    database = string
    username = string
  })
  default = {
    host     = "mooncake"
    port     = 5432
    database = "warehouse"
    username = "mooncake"
  }

  validation {
    condition     = var.warehouse_connection.port >= 1 && var.warehouse_connection.port <= 65535
    error_message = "warehouse_connection.port must be between 1 and 65535."
  }
}

variable "force_destroy_buckets" {
  description = "Allow bucket deletion with objects. Use only for disposable local environments."
  type        = bool
  default     = false
}

variable "common_tags" {
  description = "Additional tags applied to taggable resources."
  type        = map(string)
  default     = {}
}
