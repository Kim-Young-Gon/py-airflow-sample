import logging
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.plugins_manager import AirflowPlugin
from datetime import datetime
import pytz
import json

log = logging.getLogger(__name__)


class RestApiCallOperator(SimpleHttpOperator):
    def execute(self, context):
        v_first_param = context['ti'].xcom_pull(key='first_info')
        if v_first_param is not None:
            dag_id = v_first_param["dag_id"]
            hostname = v_first_param["hostname"]
            start_time = v_first_param["start_time"]
        else:
            dag_id = context['ti'].dag_id
            hostname = str(context['ti'].hostname)
            start_time = context['ti'].start_date.strftime("%Y-%m-%dT%H:%M:%S")
        duration = context['ti'].end_date - datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=pytz.utc)
        self.http_conn_id = "restapi-test"
        self.endpoint = "/kafka_api"
        self.method = "POST"
        self.headers = {"Content-Type": "application/json"}
        self.data = json.dumps({
            "topic": "kafka-test",
            "field": {
                "dag_id": dag_id,
                "hostname": hostname,
                "start_time": start_time,
                "task_id": context['task'].task_id,
                "operator": context['ti'].operator,
                "end_time": context['ti'].end_date.strftime("%Y-%m-%dT%H:%M:%S"),
                "duration": duration.seconds,
                "status": str(context['ti'].state)
            }
        })
        log.info(self.data)
        text = super(RestApiCallOperator, self).execute(context)
        return text


class RestApiCallPlugin(AirflowPlugin):
    name = "restapi_message_send_plugin"
    operators = [RestApiCallOperator]

