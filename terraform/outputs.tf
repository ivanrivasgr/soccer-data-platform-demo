output "raw_bucket_name" {
  value = aws_s3_bucket.raw_data.bucket
}

output "processed_bucket_name" {
  value = aws_s3_bucket.processed_data.bucket
}

output "analytics_bucket_name" {
  value = aws_s3_bucket.analytics_data.bucket
}