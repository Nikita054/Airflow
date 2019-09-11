from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator

from model.default_args import default_args

from initializating.initializing_functions import conditionally_trigger, init_data, init_table

dag_initializer = DAG('initializer', description='Simple tutorial DAG',
                      schedule_interval='0 * * * *',
                      default_args=default_args,
                      catchup=False)
opt_create_table = PythonOperator(task_id='create_table', python_callable=init_table,
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