variable "taxi_tables" {
  type = map(object({
    schema_file     = string
    partition_field = string
  }))
}

variable "bq_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "europe-west1"
}

variable "project_id" {
  description = "GCP project id"
  type        = string
}
