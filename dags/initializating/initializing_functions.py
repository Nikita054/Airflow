import datetime as dt
from airflow.hooks.postgres_hook import PostgresHook


def init_table():
    connection = PostgresHook(postgres_conn_id='postgres_test')
    sql = "CREATE TABLE IF NOT EXISTS users_table (user_id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL, " \
          "surname VARCHAR(255) NOT NULL, birth DATE, description VARCHAR(350));"
    connection.run(sql, autocommit=True)
    return "Table created!"


def init_data(**kwargs):
    connection = PostgresHook(postgres_conn_id='postgres_test')
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
    if c_p:
        dag_run_obj.payload = {'message': context['params']['message']}
        return dag_run_obj
    return None