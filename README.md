# NY Taxi Data Pipeline

This project is an ELT pipeline that extracts New York Taxi Trip data, loads it into Google BigQuery, and transforms it using dbt.

## Features
- **Extraction**: Downloads Parquet files from the official NYC TLC Trip Record Data.
- **Loading**: Uploads raw files to GCS and loads them into BigQuery.
- **Transformation**: Uses dbt to test and transform raw data.
- **Orchestration**: Airflow DAGs manage the workflow.
- **Date Period Support**: Can ingest individual months or a range of months (e.g., `2025-01:2025-03`).

## Project Structure
```
├── dags/               # Airflow DAGs
├── dbt/                # dbt project for transformations
├── infra/              # Terraform infrastructure code
├── scripts/            # Helper scripts
├── Dockerfile          # Docker image definition
└── docker-compose.yaml # Local development setup
```

## Prerequisites
- Docker & Docker Compose
- Google Cloud Platform Account
    - GCS Bucket
    - BigQuery Dataset
    - Service Account with appropriate permissions

## Setup

1.  **Clone the repository**:
    ```bash
    git clone <repo-url>
    cd ny_taxi_pr
    ```

2.  **Infrastructure (Optional)**:
    Use Terraform to provision GCP resources:
    ```bash
    cd infra/terraform
    terraform init
    terraform apply
    ```

3.  **Start Airflow**:
    ```bash
    docker-compose up -d
    ```

## Usage

### Running the Extractor DAG
The `ny_taxi_extractor` DAG accepts the following parameters:

-   `period`: The time period to ingest.
    -   Single month: `2025-01`
    -   Range: `2025-01:2025-03`
-   `data_type`: Taxi type (default: `yellow`).

**Example Configuration**:
```json
{
  "period": "2024-01:2024-03",
  "data_type": "yellow"
}
```

## Development
-   **Requirements**: `requirements.txt`
-   **Tests**: Run `dbt test` within the Airflow task or manually in the `dbt` directory.
