from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os

def count_lines_in_file(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    with open(file_path, 'r') as file:
        line_count = sum(1 for _ in file)
    print(f"Number of lines in {file_path}: {line_count}")
    return line_count

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'count_lines_dag',
    default_args=default_args,
    description='DAG for counting lines in a file',
    schedule_interval='@daily',
)

file_path = '/opt/airflow/dags/files/test.txt'

count_lines_task = PythonOperator(
    task_id='count_lines_task',
    python_callable=count_lines_in_file,
    op_args=[file_path],
    dag=dag,
)
