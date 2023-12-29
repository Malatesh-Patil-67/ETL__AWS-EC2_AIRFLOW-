from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from etl import run_stock_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_etl_dag',
    default_args=default_args,
    description='ETL DAG for fetching stock data and storing it on S3',
    schedule_interval=timedelta(days=1),  # Set the schedule interval as needed
)

# Define the PythonOperator to run the ETL job
run_etl_task = PythonOperator(
    task_id='run_stock_etl',
    python_callable=run_stock_etl,
    dag=dag,
)

# Set up the task dependencies


if __name__ == "__main__":
    dag.cli()
