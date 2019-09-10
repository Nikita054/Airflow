import datetime as dt
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.helpers import cross_downstream, chain

from custom.custom_operator import MyDummyOperator, MySensor
from model.default_args import default_args
from custom.custom_operator import MySkipDummyOperator

log = logging.getLogger(__name__)


def get_data(**kwargs):
    connection = PostgresHook(postgres_conn_id='postgres_default')
    sql = 'SELECT * FROM users_table'
    data = connection.get_records(sql)
    return data


def branch_func(**kwargs):
    t_i = kwargs['ti']
    condition = t_i.xcom_pull(key='condition', dag_id='initializer', include_prior_dates=True)
    if condition == "Odd":
        return 'dummy_operator2'
    elif condition == "Even":
        return ['dummy_operator1', 'dummy_operator3']
    else:
        return ['dummy_operator1', 'dummy_operator2', 'dummy_operator3']


def count_age(**kwargs):
    ti = kwargs['ti']
    print(kwargs)
    updated_data = ti.xcom_pull(key=None, task_ids='get_data')
    list_tuples = list()
    for tuple in updated_data:
        age = dt.date.today().year - tuple[3].year
        ti.xcom_push('age', age)
        list_tuples.append((tuple[0], tuple[1], tuple[2], tuple[3],
                            "My age is {}".format(age)))
    return list_tuples


def save_new_data(**kwargs):
    ti = kwargs['ti']
    tuples = ti.xcom_pull(key=None, task_ids='count_age')
    connection = PostgresHook(postgres_conn_id='postgres_default')
    sql = 'UPDATE users_table SET description=%s WHERE user_id=%s'
    for tuple in tuples:
        connection.run(sql, autocommit=True, parameters=(tuple[4], tuple[0]))
    return "YEEEES"


dag_counter = DAG('counter', description='Simple tutorial DAG',
                  schedule_interval='0 1 * * *',
                  default_args=default_args,
                  catchup=False)

# opt_custom_skip_operator = MySkipDummyOperator(my_operator_param="skip", task_id='skip_operator', dag=dag_counter)
opt_branch_operator = BranchPythonOperator(task_id='branch_task',
                                           provide_context=True,
                                           python_callable=branch_func,
                                           dag=dag_counter)
opt_custom_operator1 = MyDummyOperator(my_operator_param="1", task_id='dummy_operator1', dag=dag_counter)
opt_custom_operator2 = MyDummyOperator(my_operator_param="2", task_id='dummy_operator2', dag=dag_counter)
opt_custom_operator3 = MyDummyOperator(my_operator_param="3", task_id='dummy_operator3', dag=dag_counter)
opt_custom_operator4 = MyDummyOperator(my_operator_param="4", task_id='dummy_operator4', dag=dag_counter)
opt_custom_operator5 = MyDummyOperator(my_operator_param="5", task_id='dummy_operator5', dag=dag_counter)
opt_custom_operator6 = MyDummyOperator(my_operator_param="6", task_id='dummy_operator6', dag=dag_counter)
opr_get_data = PythonOperator(task_id='get_data', python_callable=get_data, provide_context=True, dag=dag_counter)
opt_count_age = PythonOperator(task_id='count_age', python_callable=count_age, trigger_rule='none_failed',
                               provide_context=True, dag=dag_counter)
opt_save_data = PythonOperator(task_id='save_data', python_callable=save_new_data, provide_context=True,
                               dag=dag_counter)
opt_sensor = MySensor(task_id='sensor_task', poke_interval=1, provide_context=True, dag=dag_counter)

chain(opr_get_data, opt_branch_operator, [opt_custom_operator1,
                                          opt_custom_operator2,
                                          opt_custom_operator3], [opt_custom_operator4,
                                                                  opt_custom_operator5,
                                                                  opt_custom_operator6], opt_count_age, opt_save_data)
opt_sensor
