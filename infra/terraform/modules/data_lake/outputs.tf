output "buckets" {
  description = "Lake buckets keyed by zone."
  value = {
    for key, bucket in aws_s3_bucket.this : key => {
      id   = bucket.id
      name = bucket.bucket
      arn  = bucket.arn
    }
  }
}
