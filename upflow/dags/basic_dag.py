from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Upgrade',
    'start_date': datetime(2017, 12, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False
}

dag = DAG('basic_dag', default_args=default_args)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = BashOperator(
    task_id='sleep_5_seconds',
    bash_command='sleep 5',
    retries=3,
    dag=dag)


t3 = PythonOperator(
       task_id="Some_python_task",
       python_callable=lambda: "do_nothing",
       dag=dag
     )

t2.set_upstream(t1)
t3.set_upstream(t2)
