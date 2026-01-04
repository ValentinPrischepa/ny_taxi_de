# terraform.tfvars
project_id = "nfrowner1"
region     = "europe-west1"

taxi_tables = {
  fhv = {
    schema_file     = "schemas/fhv_trips.json"
    partition_field = "pickup_datetime"
  }
  yellow = {
    schema_file     = "schemas/yellow_trips.json"
    partition_field = "tpep_pickup_datetime"
  }
  green = {
    schema_file     = "schemas/green_trips.json"
    partition_field = "lpep_pickup_datetime"
  }
}

bq_location     = "europe-west1"
