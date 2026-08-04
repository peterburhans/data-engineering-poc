variable "bucket_names" {
  description = "Physical names keyed by lake zone."
  type = object({
    raw     = string
    curated = string
  })
}

variable "force_destroy" {
  description = "Allow deletion of non-empty buckets in disposable environments."
  type        = bool
  default     = false
}
