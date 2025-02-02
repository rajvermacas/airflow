from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

def check_day():
    """Function that determines which task to run based on even/odd day"""
    current_day = datetime.now().day
    if current_day % 2 == 0:
        return 'even_day_group.even_day_task'  # Return task group ID instead of task ID
    return 'odd_day_group.odd_day_task'

def even_day_task():
    """Task that runs on even days"""
    print("Running task for even day!")
    return "Even day task completed"

def odd_day_task():
    """Task that runs on odd days"""
    print("Running task for odd day!")
    return "Odd day task completed"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'even_odd_day_dag',
    default_args=default_args,
    description='A DAG that runs different tasks for even and odd days',
    schedule_interval='@daily',
    catchup=False
) as dag:

    start = EmptyOperator(
        task_id='start'
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=check_day
    )

    # Create task groups for even and odd days
    with TaskGroup(group_id='even_day_group') as even_group:
        even_task = PythonOperator(
            task_id='even_day_task',
            python_callable=even_day_task
        )
        
        # You can add more tasks in the even day group
        even_extra = EmptyOperator(task_id='even_extra')
        even_task >> even_extra

    with TaskGroup(group_id='odd_day_group') as odd_group:
        odd_task = PythonOperator(
            task_id='odd_day_task',
            python_callable=odd_day_task
        )
        
        # You can add more tasks in the odd day group
        odd_extra = EmptyOperator(task_id='odd_extra')
        odd_task >> odd_extra

    end = EmptyOperator(
        task_id='end',
        trigger_rule='none_failed'  # Ensures end runs regardless of which branch was taken
    )

    # Set up the task dependencies
    start >> branch_task >> [even_group, odd_group] >> end