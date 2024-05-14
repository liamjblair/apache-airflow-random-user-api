from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from gather_user_data import main
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 5, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "random_user_api",
    default_args=default_args,
    description="Grabs 100 random users data from an API daily",
    schedule_interval="@daily"
)

with dag:
    task = PythonOperator(
        task_id = "main",
        python_callable = main,
        provide_context=True
    )

