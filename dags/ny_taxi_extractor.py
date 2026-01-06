import base64
import hashlib
import tempfile
from datetime import datetime
from airflow.models import Param
from airflow import DAG
from airflow.decorators import task
import requests

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.bash import BashOperator
from google.cloud.bigquery import LoadJobConfig, SourceFormat

from constants import NY_BQ_DATASET, NY_RAW_BUCKET, GC_PROJECT_ID

with DAG(
    dag_id="ny_taxi_extractor",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "period": Param(
            default="2025-03",
            type="string",
            description="Year-month to ingest, e.g. 2025-01 or period like 2025-01:2025-03"
        ),
        "data_type": Param(
                    default="yellow",
                    type="string",
                    description="Data type to ingest, e.g. yellow"
                )
    }
) as dag:
    @task
    def get_year_months(period: str) -> list[str]:
        if ":" not in period:
            return [period]

        start_str, end_str = period.split(":")
        start_date = datetime.strptime(start_str, "%Y-%m")
        end_date = datetime.strptime(end_str, "%Y-%m")

        months = []
        current_date = start_date
        while current_date <= end_date:
            months.append(current_date.strftime("%Y-%m"))
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)
        return months


    @task
    def download_parquet(year_month: str, data_type: str) -> str:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{data_type}_tripdata_{year_month}.parquet"

        hook = GCSHook(gcp_conn_id="google_cloud_default")

        md5 = hashlib.md5()

        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with tempfile.NamedTemporaryFile() as tmp:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        tmp.write(chunk)
                        md5.update(chunk)
                tmp.flush()
                local_md5 = md5.hexdigest()
                object_name = f"{data_type}/{year_month}.parquet"
                hook.upload(
                    bucket_name=NY_RAW_BUCKET,
                    object_name=object_name,
                    filename=tmp.name)

                client = hook.get_conn()
                bucket = client.bucket(NY_RAW_BUCKET)
                blob = bucket.blob(object_name)

                blob.reload()

                gcs_md5 = base64.b64decode(blob.md5_hash).hex()

                if local_md5 != gcs_md5:
                    raise ValueError(
                        f"Checksum mismatch: local={local_md5}, gcs={gcs_md5}"
                    )
        return object_name

    @task
    def load_gcs_to_bq(
            gcp_conn_id: str,
            bucket: str,
            project_id: str,
            dataset: str,
            data_type: str,
            object_names: list[str],
    ):
        bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
        client = bq_hook.get_client()

        table_id = f"{project_id}.{dataset}.{data_type}_trips"
        uris = [f"gs://{bucket}/{obj}" for obj in object_names]

        job_config = LoadJobConfig(
            source_format=SourceFormat.PARQUET,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE",
        )

        load_job = client.load_table_from_uri(
            uris,
            table_id,
            job_config=job_config,
        )
        load_job.result()

        return {"table_id": table_id, "loaded_rows": load_job.output_rows}


    dbt_test_raw = BashOperator(
        task_id="dbt_test_raw_sources",
        bash_command="""
        cd /opt/airflow/dbt/ny_taxi_dbt &&
        DBT_PROFILES_DIR=. /home/airflow/.local/bin/dbt test --select source:ny_taxi_raw
        """,
    )

    year_months = get_year_months("{{ params.period }}")


    download_tasks = download_parquet.partial(data_type="{{ params.data_type }}").expand(year_month=year_months)

    load_gcs_to_bq_task = load_gcs_to_bq(
        gcp_conn_id="google_cloud_default",
        bucket=NY_RAW_BUCKET,
        project_id=GC_PROJECT_ID,
        dataset=NY_BQ_DATASET,
        data_type="{{ params.data_type }}",
        object_names=download_tasks
    )

    load_gcs_to_bq_task >> dbt_test_raw