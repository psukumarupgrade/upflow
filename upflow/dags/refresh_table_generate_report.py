from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from upflow.helpers.postgres import PostgresHook, PostgresRunSQLOperator, PostgresUnloaderOperator
from upflow.helpers.slack import SlackAPIPostOperator
from upflow.helpers.edw import EDW
from airflow.models import DAG
from airflow.utils import dates
from datetime import timedelta


default_args = {
    'owner': 'Upgrade',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': dates.days_ago(2)
}

dag = DAG(
    dag_id='report_refresh',
    default_args=default_args,
    schedule_interval="@daily")


start = DummyOperator(task_id='Start', dag=dag)

refresh_table = PostgresRunSQLOperator(
    task_id="Refresh_Table_Generate_Report",
    dag=dag,
    sql="SELECT count(1) FROM loanreview.loan_in_review LIMIT 10",
    postgres_conn_id=EDW.get_conn_id(),
    execution_timeout=timedelta(minutes=30)
)


def is_data_valid():
    pg_hook = PostgresHook(postgres_conn_id=EDW.get_conn_id(),
                           schema=EDW.get_default_schema())
    validation_sql = """SELECT count(1) AS  rowcount 
                        FROM 
                        loanreview.loan_in_review WHERE cast(update_date as DATE) = current_date - 1"""
    df = pg_hook.get_pandas_df(validation_sql, lowercase_columns=True)
    if df["rowcount"].iloc[0] > 0:
        return True
    else:
        return False


validate_data = BranchPythonOperator(
    task_id='Validate_Data',
    python_callable=lambda: "export_investor_report_s3" if is_data_valid() else "notify_validation_failure",
    dag=dag)


export_report = PostgresUnloaderOperator(
    task_id="export_investor_report_s3",
    dag=dag,
    postgres_conn_id=EDW.get_conn_id(),
    source="select * from loanreview.loan_in_review WHERE cast(update_date as DATE) = current_date - 1",
    uri=EDW.get_s3_stage_uri(path="lir.csv"),
    execution_timeout=timedelta(minutes=30)
)

notify_slack_validation_fail = SlackAPIPostOperator(
    message="Validation failure. Cannot export {0} to s3".format("investor report"),
    task_id="notify_validation_failure",
    dag=dag,
    execution_timeout=timedelta(minutes=30)
)

start.set_downstream(refresh_table)
refresh_table.set_downstream(validate_data)
validate_data.set_downstream([export_report, notify_slack_validation_fail])
