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
    schedule=None,  # manual trigger
    catchup=False,
    params={
        "year_month": Param(
            default="2025-01",
            type="string",
            description="Year-month to ingest, e.g. 2025-01"
        ),
        "data_type": Param(
                    default="yellow",
                    type="string",
                    description="Data type to ingest, e.g. yellow"
                )
    }
) as dag:
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
            object_name: str,
    ):
        bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
        client = bq_hook.get_client()

        table_id = f"{project_id}.{dataset}.{data_type}_trips"
        uri = f"gs://{bucket}/{object_name}"

        job_config = LoadJobConfig(
            source_format=SourceFormat.PARQUET,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE",
        )

        load_job = client.load_table_from_uri(
            uri,
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

    year_month = dag.params["year_month"]
    data_type = dag.params["data_type"]

    download_parquet_task = download_parquet(year_month, data_type)

    load_gcs_to_bq_task = load_gcs_to_bq(
        gcp_conn_id="google_cloud_default",
        bucket=NY_RAW_BUCKET,
        project_id=GC_PROJECT_ID,
        dataset=NY_BQ_DATASET,
        data_type="{{ params.data_type }}",
        object_name=download_parquet_task
    )


    download_parquet_task >> load_gcs_to_bq_task >> dbt_test_raw