from upflow.helpers.dag import *
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from airflow.utils import dates
import os

dag = initialize_dag(dag_id='data_mart_loader',
                     schedule_interval=timedelta(hours=1),
                     concurrency=5, max_active_runs=1,
                     start_date=dates.days_ago(2))

start_dummy = DummyOperator(
    task_id='Start',
    dag=dag,
)

postgres_conn_id = 'edw_connection'
datamart_sql_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../sqls/data_mart/")
build_sql_workflow_from_directory(directory=datamart_sql_directory,
                                  dag=dag,
                                  connection_id=postgres_conn_id,
                                  upstream_task=start_dummy,
                                  default_task_timeout_minutes=50
                                  )
