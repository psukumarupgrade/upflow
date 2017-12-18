import sys
from airflow.hooks.postgres_hook import PostgresHook

dag_input = sys.argv[1]
hook = PostgresHook( postgres_conn_id="airflow_db")
table_list = ["xcom", "task_instance", "sla_miss", "log", "job", "dag_run", "dag", "task_fail", "dag_stats"]
for t in table_list:
    if dag_input == "all":
        sql = "delete from {} where 1=1".format(t)
    else:
        sql = "delete from {} where dag_id='{}'".format(t, dag_input)
    hook.run(sql, True)
