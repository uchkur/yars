from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'manasi',
    'depends_on_past': False,
    'start_date': datetime(2016, 4, 15),
    'email': ['manasidalvi14@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

#dag = DAG('Helloworld', default_args=default_args)

with DAG (dag_id='Helloworld', default_args=default_args, schedule_interval='0 0 * * *') as dag:
    # t1, t2, t3 and t4 are examples of tasks created using operators

    t1 = BashOperator(
            task_id='task_1',
            bash_command='echo "Hello World from Task 1"'
            )

    t2 = BashOperator(
            task_id='task_2',
            bash_command='echo "Hello World from Task 2"'
            )

    t3 = BashOperator(
            task_id='task_3',
            bash_command='echo "Hello World from Task 3"'
            )

    t4 = BashOperator(
            task_id='task_4',
            bash_command='echo "Hello World from Task 4"'
            )

t1>>[t2, t3, t4]
