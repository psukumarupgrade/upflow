from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from random import randint
import math

default_args = {
    'owner': 'Upgrade',
    'start_date': datetime(2017, 12, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False
}

dag = DAG('dynamic_dag', default_args=default_args)


def num_events_in_queue():
    return randint(0, 200000)


def num_tasks_to_generate():
    num_events_per_task = 20000
    return int(math.ceil(num_events_in_queue() / num_events_per_task))


def process_queue():
    pass


start_dummy = DummyOperator(
    task_id='Start',
    dag=dag,
)

for t in range(num_tasks_to_generate()):
    task = PythonOperator(
        task_id="Some_python_task_" + str(t),
        python_callable=process_queue,
        dag=dag
    )
    start_dummy.set_downstream(task)

