import os
from airflow.models import DAG
from upflow.helpers.postgres import PostgresRunSQLOperator
from airflow.operators.dummy_operator import DummyOperator
from upflow.helpers.slack import SlackHook, SlackAPIPostOperator
from upflow.helpers.sql import sql_tag_evaluate
from upflow.helpers.file import read_file_contents
from datetime import timedelta, datetime


def initialize_dag(dag_id, schedule_interval, concurrency=3,
                   depends_on_past=False, retry_delay=timedelta(minutes=1),
                   dagrun_timeout=timedelta(hours=5), max_active_runs=1,
                   on_failure_callback=True, on_retry_callback=False,
                   on_success_callback=True, max_retries=3,
                   start_date=None, severity=None
                   ):
    if type(schedule_interval) != timedelta:
        raise ValueError('Schedule interval must be a timedelta')

    # Start one interval ago so there is one running.
    if not start_date:
        start_date = datetime.utcnow() - schedule_interval
        # If its interval is greater than or equal to an hour, make sure it starts at the top of the hour.
        if (schedule_interval.total_seconds() / 60.0) >= 60:
            start_date = start_date.replace(minute=0, second=0, microsecond=0)
            # If its greater than a day interval make it start at midnight.
            if schedule_interval.days >= 1:
                start_date = start_date.replace(hour=0)

    # Maximum 3 retries.
    retries = min(int(((schedule_interval.total_seconds() / 60.0) - (retry_delay.total_seconds() / 60.0)) /
                      (retry_delay.total_seconds() / 60.0)), max_retries)

    default_args = {
        'owner': 'Upgrade Data Engineering',
        'retry_delay': retry_delay,
        'dagrun_timeout': dagrun_timeout,
        'retries': retries,
        'start_date': start_date,
        'depends_on_past': depends_on_past
    }
    params = {}
    if severity:
        params["severity"] = severity

    if on_retry_callback:
        default_args['on_retry_callback'] = on_retry
    if on_failure_callback:
        default_args['on_failure_callback'] = on_failure
    if on_success_callback:
        default_args['on_success_callback'] = on_success

    dag = DAG(
        dag_id=dag_id,
        schedule_interval=schedule_interval,
        default_args=default_args,
        concurrency=concurrency,
        max_active_runs=max_active_runs,
        params=params

    )
    return dag


def on_failure(context, *op_args, **op_kwargs):
    task_object = context['ti']
    execution_date = task_object.execution_date.strftime('%Y-%m-%d-%H-%M-%S')
    message = """Airflow task failure for \nDAG: {0}\nTASK: {1}\nEXECUTION_DATE: {2}\n"""\
        .format(task_object.dag_id, task_object.task_id, execution_date)
    slack_handler = SlackHook()
    slack_handler.send_slack_message(message)


def on_retry(context, *op_args, **op_kwargs):
    pass


def on_success(context, *op_args, **op_kwargs):
    pass


def traverse_directory_recursively_for_files(path, extension=None):
    output = []
    for root, directories, files in os.walk(path):
        for filename in files:
            if not extension:
                output.append(os.path.join(root, filename))
            elif filename.lower().endswith(extension.lower()):
                output.append(os.path.join(root, filename))
    return output


def build_sql_workflow_from_directory(directory, dag, connection_id, upstream_task=None,
                                      downstream_task=None, default_task_timeout_minutes=50,
                                      task_name_suffix="", autocommit=True, parameters=None,
                                      dependency_regex='-- DEPENDS_ON:',
                                      **kwargs):
    files = traverse_directory_recursively_for_files(directory, '.sql')
    dependent_files = {}
    file_name_to_operator_lookup = {}
    is_broken = False
    broken_messages = []
    if not upstream_task:
        upstream_task = DummyOperator(
            task_id='Start',
            dag=dag,
        )
    for sql_file in files:
        with open(sql_file) as f:
            first_line = f.readline()
        task_name = os.path.basename(sql_file)[:-len('.sql')] + task_name_suffix
        temp = PostgresRunSQLOperator(
            task_id=task_name,
            dag=dag,
            sql=sql_tag_evaluate(read_file_contents(sql_file), **kwargs),
            postgres_conn_id=connection_id,
            autocommit=autocommit,
            parameters=parameters,
            execution_timeout=timedelta(minutes=default_task_timeout_minutes),
            **kwargs
        )
        # If it is a dependent file, keep track of what it depends on.
        if first_line.startswith(dependency_regex):
            dependent_files[sql_file] = [os.path.basename(parent_file.strip())
                                         for parent_file in first_line[len(dependency_regex):].split(',')]
        else:
            temp.set_upstream(upstream_task)
        file_name_to_operator_lookup[os.path.basename(sql_file)] = temp

    # Set the dependency for the dependent files.
    for child_file, parent_list in dependent_files.items():
        for parent_file_name in parent_list:
            if os.path.basename(parent_file_name) in file_name_to_operator_lookup:
                file_name_to_operator_lookup[os.path.basename(child_file)] \
                    .set_upstream(file_name_to_operator_lookup[os.path.basename(parent_file_name)])
            else:
                is_broken = True
                #file_name_to_operator_lookup[os.path.basename(child_file)].set_upstream(upstream_task)
                broken_messages.append('Attempting to set: {0}\nAs being dependent on: {1}\n'
                                       'But {2} is not found'.format(child_file,
                                                                    parent_file_name,
                                                                    parent_file_name))

    # If a downstream task is supplied. Only point the tasks that have no dependencies to the downstream task.
    if downstream_task is not None:
        all_files_base = [os.path.basename(f) for f in files]
        parent_files = []
        [parent_files.extend(parents) for child, parents in dependent_files.items()]
        to_point_downstream = list(set(all_files_base) - set(parent_files))
        for file_name in to_point_downstream:
            downstream_task.set_upstream(file_name_to_operator_lookup[file_name])

    # Build a task to alert if the dependency chain is broken.
    if is_broken:
        message = 'Broken SQL dependency chain in directory: {0}\n'.format(directory)
        message += '\n'.join(broken_messages)
        sns_object = SlackAPIPostOperator(
            text=message,
            task_id='alert_on_broken_chain',
            dag=dag
        )
        sns_object.set_upstream(upstream_task)
