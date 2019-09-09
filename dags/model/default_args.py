import datetime as dt


default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2016, 10, 3, 15, 00, 00),
    'retries': 2
}