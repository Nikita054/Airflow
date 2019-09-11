import datetime as dt
import logging
from airflow.hooks.postgres_hook import PostgresHook


def get_data(**kwargs):
    connection = PostgresHook(postgres_conn_id='postgres_test')
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
    connection = PostgresHook(postgres_conn_id='postgres_test')
    sql = 'UPDATE users_table SET description=%s WHERE user_id=%s'
    for tuple in tuples:
        connection.run(sql, autocommit=True, parameters=(tuple[4], tuple[0]))
    return "YEEEES"


