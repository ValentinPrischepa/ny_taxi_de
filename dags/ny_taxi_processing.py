from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="ny_taxi_processing",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    dbt_seed_reference_data = BashOperator(
        task_id="dbt_seed_reference_data",
        bash_command="""
        cd /opt/airflow/dbt/ny_taxi_dbt &&
        DBT_PROFILES_DIR=. /home/airflow/.local/bin/dbt seed
        """,
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="""
        cd /opt/airflow/dbt/ny_taxi_dbt &&
        DBT_PROFILES_DIR=. /home/airflow/.local/bin/dbt run --models +daily_trip_metrics dim_zone
        """,
    )

    dbt_seed_reference_data >> dbt_run_marts