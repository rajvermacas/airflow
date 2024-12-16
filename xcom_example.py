from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

# Define the function to run script1.py
def run_script1(**context):
    # Pushing a value to XCom
    task_instance = context['task_instance']
    task_instance.xcom_push(key='message_from_task1', value='Hello from Task 1!')
    print("I am task 1")

# Define the function to run script2.py
def run_script2(**context):
    # Pulling value from XCom
    task_instance = context['task_instance']
    message = task_instance.xcom_pull(task_ids='run_script1', key='message_from_task1')
    print(f"I am task 2, and I received: {message}")
    
    # Push another value
    task_instance.xcom_push(key='task2_completion_time', value=str(datetime.now()))

# Define the DAG
dag = DAG(
    'xcom_example',
    description='Run tasks sequentially and trigger new run after completion',
    schedule_interval=None,  # Only manual triggers or from TriggerDagRunOperator
    start_date=datetime(2024, 11, 15),
    catchup=False,
)

# Define the tasks
task1 = PythonOperator(
    task_id='run_script1',
    python_callable=run_script1,
    provide_context=True,  # This is needed to receive the context dictionary
    dag=dag,
)

task2 = PythonOperator(
    task_id='run_script2',
    python_callable=run_script2,
    provide_context=True,  # This is needed to receive the context dictionary
    dag=dag,
)

# Task to trigger the next DAG run
trigger_next_dag_run = TriggerDagRunOperator(
    task_id='trigger_next_dag_run',
    trigger_dag_id='xcom_example',
    dag=dag,
)

# Set task dependencies: task1 -> task2 -> trigger_next_dag_run
task1 >> task2 >> trigger_next_dag_run