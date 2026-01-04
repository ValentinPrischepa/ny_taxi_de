resource "google_bigquery_table" "taxi_trips" {
  for_each = var.taxi_tables

  project    = var.project_id
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "${each.key}_trips"

  schema = file("${path.module}/${each.value.schema_file}")

  time_partitioning {
    type  = "MONTH"
    field = each.value.partition_field
  }

  deletion_protection = false
}
