provider "google" {
  project = var.project_id
  region  = var.region
}
resource "google_storage_bucket" "raw_bucket" {
  name          = "${var.project_id}-raw"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

module "bigquery" {
  source = "./bigquery"

  project_id   = var.project_id
  bq_location  = var.bq_location
  taxi_tables  = var.taxi_tables
}
