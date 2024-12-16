from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import subprocess

# Define the function to run script1.py
def run_script1():
    subprocess.run(['python', '/path/to/script1.py'], check=True)

# Define the function to run script2.py
def run_script2():
    subprocess.run(['python', '/path/to/script2.py'], check=True)

# Define the DAG
dag = DAG(
    'run_scripts_sequentially',  # DAG name
    description='Run script1.py followed by script2.py',
    schedule_interval=None,  # No scheduling, will run manually
    start_date=datetime(2024, 11, 15),
    catchup=False,  # Avoid running past missed schedules
)

# Define the tasks
task1 = PythonOperator(
    task_id='run_script1',  # Task name
    python_callable=run_script1,  # Function to run
    dag=dag,
)

task2 = PythonOperator(
    task_id='run_script2',  # Task name
    python_callable=run_script2,  # Function to run
    dag=dag,
)

# Set the order of execution: task1 must run before task2
task1 >> task2  # task1 runs first, followed by task2
