from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

# Define the function to run script1.py
def run_script1():
    print("I am task 1")

# Define the function to run script2.py
def run_script2():
    print("I am task 2")

# Define the DAG
dag = DAG(
    'self_triggering_dag',
    description='Run tasks sequentially and trigger new run after completion',
    schedule_interval=None,  # Only manual triggers or from TriggerDagRunOperator
    start_date=datetime(2024, 11, 15),
    catchup=False,
)

# Define the tasks
task1 = PythonOperator(
    task_id='run_script1',
    python_callable=run_script1,
    dag=dag,
)

task2 = PythonOperator(
    task_id='run_script2',
    python_callable=run_script2,
    dag=dag,
)

# Task to trigger the next DAG run
trigger_next_dag_run = TriggerDagRunOperator(
    task_id='trigger_next_dag_run',
    trigger_dag_id='self_triggering_dag',  # Same DAG ID as current DAG
    dag=dag,
)

# Set task dependencies: task1 -> task2 -> trigger_next_dag_run
task1 >> task2 >> trigger_next_dag_run
