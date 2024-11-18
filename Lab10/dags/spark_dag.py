from airflow import DAG 
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from datetime import datetime
from datetime import timedelta

 
default_args = { 
    'owner': 'airflow', 
    'start_date': datetime(2024, 11, 13), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5) 
} 
dag = DAG( 
    'spark_job', 
    default_args=default_args, 
    description='DAG to run Spark jobs', 
    schedule_interval='@daily', 
) 
spark_task = SparkSubmitOperator( 
    task_id='spark_task', 
    application='/opt/airflow/dags/spark_job.py',
    conn_id='spark_default', 
    dag=dag, 
)