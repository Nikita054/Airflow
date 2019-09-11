import logging
from airflow.models import TaskInstance
from airflow.operators.sensors import BaseSensorOperator

log = logging.getLogger(__name__)


class MySensor(BaseSensorOperator):
    def __init__(self, *args, **kwargs):
        super(MySensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        task_instance = context['ti']
        age = task_instance.xcom_pull(key='age', task_ids='count_age')
        dag_instance = context['dag']
        operator_instance = dag_instance.get_task("count_age")
        task_status = TaskInstance(operator_instance, context['execution_date']).current_state()
        if age is not None and age > 70:
            return True
        if task_status == 'success':
            return True
        elif task_status == 'failed' or task_status == 'skipped':
            log.info("Count_age is failed or skipped!")
            return True
        log.info("Age lower than 70!")
        return False
