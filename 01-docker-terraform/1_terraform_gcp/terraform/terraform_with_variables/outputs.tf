# GCP Data Lake
output "gcp_data_lake" {
  value = google_storage_bucket.demo-bucket.name
}

# GCP Data Warehouse
output "gcp_data_warehouse" {
  value = google_bigquery_dataset.demo_dataset.dataset_id
}
