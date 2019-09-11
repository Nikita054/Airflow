from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator, TaskInstance
import logging
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
