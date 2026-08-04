variable "names" {
  description = "Canonical physical names for streaming resources."
  type = object({
    event_stream    = string
    delivery_stream = string
    firehose_role   = string
    firehose_policy = string
  })
}

variable "raw_bucket" {
  description = "Raw-zone S3 bucket identity."
  type = object({
    id   = string
    name = string
    arn  = string
  })
}

variable "raw_prefix" {
  description = "Raw-zone object prefix for this event type."
  type        = string
}

variable "event_time_field" {
  description = "JSON timestamp field used for event-time partition extraction."
  type        = string
}

variable "buffering" {
  description = "Extended S3 buffering thresholds controlling Firehose delivery latency."
  type = object({
    interval_seconds = number
    size_mb          = number
  })

  validation {
    condition = (
      var.buffering.interval_seconds >= 0 &&
      var.buffering.interval_seconds <= 900 &&
      var.buffering.size_mb >= 1 &&
      var.buffering.size_mb <= 128
    )
    error_message = "buffering interval must be 0-900 seconds and size must be 1-128 MB."
  }
}
