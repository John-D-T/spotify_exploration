"""
apache-airflow version: 2.7.3

Steps to initialize and start airflow:

airflow db init
    - initializes airflow db
    - To revert this (when you're done, run: airflow db reset)
airflow webserver -p 8080
    - start airflow webserver
    - if running on windows, you'll get an error:
            'no module named "pwd"'
            SOLN: Use Windows Subsystem for Linux
airflow scheduler
    - start airflow scheduler

airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
    - Creating your first user
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import mysql.connector
import random
from datetime import datetime, timedelta
from pipeline.main import full_pipeline

# Define default_args and DAG
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'website_traffic_etl',
    default_args=default_args,
    description='ETL process for website traffic data',
    schedule_interval=timedelta(minutes=15),  # Run every 15 minutes
    catchup=False,
)


# Define the ETL task
full_pipeline_task = PythonOperator(
    task_id='full_pipeline_task',
    python_callable=full_pipeline,
    dag=dag,
)

# TODO - Set task dependencies here (when I have more than one operator)

if __name__ == "__main__":
    dag.cli()