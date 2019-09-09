from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator
import logging

from airflow.operators.sensors import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class MyDummyOperator(BaseOperator):
    @apply_defaults
    def __init__(self, my_operator_param, *args, **kwargs):
        self.operator_param = my_operator_param
        super(MyDummyOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("hello from custom operator")
        log.info("my number is: %s", self.operator_param)


class MySkipDummyOperator(BaseOperator):
    @apply_defaults
    def __init__(self, my_operator_param, *args, **kwargs):
        self.operator_param = my_operator_param
        super(MySkipDummyOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        raise AirflowSkipException


class MySensor(BaseSensorOperator):
    def __init__(self, *args, **kwargs):
        super(MySensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        task_instance = context['ti']
        age = task_instance.xcom_pull(key='age', task_ids='count_age')
        print("Age is {}".format(age))
        if age is not None and age > 70:
            return True
        print("Age lower than 70!")
        return False


class MyPlugin(AirflowPlugin):
    name = "My plugin"
    operators = [MyDummyOperator, MySensor]
