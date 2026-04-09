output "bucket_name" {
  description = "GCS Bucket Name"
  value       = google_storage_bucket.data_lake.name
}

output "raw_data_dataset" {
  description = "Raw Data Dataset ID"
  value       = google_bigquery_dataset.raw_data.dataset_id
}

output "staging_dataset" {
  description = "Staging Dataset ID"
  value       = google_bigquery_dataset.staging.dataset_id
}

output "marts_dataset" {
  description = "Marts Dataset ID"
  value       = google_bigquery_dataset.marts.dataset_id
}
