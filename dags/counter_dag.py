from itertools import chain

from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from model.default_args import default_args
from computing.functions_of_computing import save_new_data, count_age, get_data, branch_func
from custom.custom_operator import MyDummyOperator
from custom.custom_sensor import MySensor

dag_counter = DAG('counter', description='Simple tutorial DAG',
                  schedule_interval='0 1 * * *',
                  default_args=default_args,
                  catchup=False)

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
opt_get_data = PythonOperator(task_id='get_data', python_callable=get_data, provide_context=True, dag=dag_counter)
opt_count_age = PythonOperator(task_id='count_age', python_callable=count_age, trigger_rule='none_failed',
                               provide_context=True, dag=dag_counter)
opt_save_data = PythonOperator(task_id='save_data', python_callable=save_new_data, provide_context=True,
                               dag=dag_counter)
opt_sensor = MySensor(task_id='sensor_task', poke_interval=1, provide_context=True, dag=dag_counter)


chain(opt_get_data, opt_branch_operator, [opt_custom_operator1,
                                          opt_custom_operator2,
                                          opt_custom_operator3], [opt_custom_operator4,
                                                                  opt_custom_operator5,
                                                                  opt_custom_operator6], opt_count_age, opt_save_data)
opt_sensor

