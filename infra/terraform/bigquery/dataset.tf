resource "google_bigquery_dataset" "raw" {
  dataset_id = "ny_taxi_raw"
  project    = var.project_id
  location   = var.bq_location

  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "staging" {
  dataset_id = "ny_taxi_staging"
  project    = var.project_id
  location   = var.bq_location

  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "marts" {
  dataset_id = "ny_taxi_marts"
  project    = var.project_id
  location   = var.bq_location

  delete_contents_on_destroy = true
}