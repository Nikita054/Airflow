import datetime as dt
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models.variable import Variable

from airflow.models.dagrun import DagRun
from pprint import pprint

from model.default_args import default_args

log = logging.getLogger(__name__)
execution_date = None
Variable.set('execution_date', None)
Variable.set('trigger', None)


def init_table(**kwargs):
    connection = PostgresHook(postgres_conn_id='postgres_default')
    sql = "CREATE TABLE IF NOT EXISTS users_table (user_id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL, " \
          "surname VARCHAR(255) NOT NULL, birth DATE, description VARCHAR(350));"
    connection.run(sql, autocommit=True)
    return "Table created!"


def init_data(**kwargs):
    connection = PostgresHook(postgres_conn_id='postgres_default')
    sql = 'INSERT INTO users_table (name, surname, birth) VALUES (%s, %s, %s)'
    name = 'Default name'
    surname = 'Default surname'
    t_i = kwargs['ti']
    t_i.xcom_push('condition', 'Odd')
    for i in range(100):
        birth = dt.datetime(dt.datetime.today().year - i, 1, 10)
        connection.run(sql, autocommit=True, parameters=(name, surname, birth))
    return "Data inserted"


def conditionally_trigger(context, dag_run_obj):
    c_p = context['params']['condition_param']
    t_i = context['ti']
    condition = t_i.xcom_pull(key='condition', task_ids='init_data')
    if c_p:
        # pprint(vars(dag_run_obj))
        # dag_run_obj.execution_date = context['execution_date']
        # log.info(dag_run_obj.execution_date)
        dag_run_obj.payload = {'message': context['params']['message'], 'condition': condition}
        return dag_run_obj
    return None


dag_initializer = DAG('initializer', description='Simple tutorial DAG',
                      schedule_interval='0 * * * *',
                      default_args=default_args,
                      catchup=False)

opt_create_table = PythonOperator(task_id='create_table', python_callable=init_table, provide_context=True,
                                  dag=dag_initializer)
opt_init_data = PythonOperator(task_id='init_data', python_callable=init_data, provide_context=True,
                               dag=dag_initializer)
trigger = TriggerDagRunOperator(
    task_id='example_of_trigger', trigger_dag_id='counter',
    python_callable=conditionally_trigger,
    provide_context=True,
    params={'condition_param': True, 'message': 'Hello World'},
    dag=dag_initializer
)

opt_create_table >> opt_init_data >> trigger
