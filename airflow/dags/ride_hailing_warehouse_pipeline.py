from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = "/opt/ride-hailing"


default_args = {
    "owner": "walter",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="ride_hailing_warehouse_pipeline",
    description="Load clean ride events from MinIO into PostgreSQL warehouse star schema",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/15 * * * *",
    catchup=False,
    tags=["ride-hailing", "spark", "warehouse"],
) as dag:

    load_clean_events_to_staging = BashOperator(
        task_id="load_clean_events_to_staging",
        bash_command=f"""
        /opt/spark/bin/spark-submit \
        {PROJECT_ROOT}/spark/load_clean_ride_events_to_postgres.py
        """,
    )

    load_dim_driver = BashOperator(
        task_id="load_dim_driver",
        bash_command=f"python {PROJECT_ROOT}/warehouse/load_dim_driver.py",
    )

    load_dim_rider = BashOperator(
        task_id="load_dim_rider",
        bash_command=f"python {PROJECT_ROOT}/warehouse/load_dim_rider.py",
    )

    load_dim_location = BashOperator(
        task_id="load_dim_location",
        bash_command=f"python {PROJECT_ROOT}/warehouse/load_dim_location.py",
    )

    load_dim_payment_method = BashOperator(
        task_id="load_dim_payment_method",
        bash_command=f"python {PROJECT_ROOT}/warehouse/load_dim_payment_method.py",
    )

    load_dim_date = BashOperator(
        task_id="load_dim_date",
        bash_command=f"python {PROJECT_ROOT}/warehouse/load_dim_date.py",
    )

    load_fct_ride_events = BashOperator(
        task_id="load_fct_ride_events",
        bash_command=f"python {PROJECT_ROOT}/warehouse/load_staging_to_warehouse.py",
    )

    update_fct_ride_events_dimension_keys = BashOperator(
        task_id="update_fct_ride_events_dimension_keys",
        bash_command=f"python {PROJECT_ROOT}/warehouse/update_fct_ride_events_dimension_keys.py",
    )

    load_clean_events_to_staging >> [
        load_dim_driver,
        load_dim_rider,
        load_dim_location,
        load_dim_payment_method,
        load_dim_date,
    ] >> load_fct_ride_events >> update_fct_ride_events_dimension_keys
