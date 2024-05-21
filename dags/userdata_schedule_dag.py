from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from gather_random_user_data_run import run
# from userdata_test import main
from datetime import datetime, timedelta


dag_args ={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        }

dag = DAG (
    dag_id="random_user_api",
    schedule_interval="@daily",
    default_args=dag_args,
    catchup=False,
    description="random_user_api"
)

task = PythonOperator(
        task_id="gather_user_data",
        python_callable=run,
        dag=dag
    )

task