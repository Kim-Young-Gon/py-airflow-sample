# -*- coding: utf-8 -*-

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule
try:
    from RestApiCallOperator import RestApiCallOperator
except ImportError:
    from airflow.operators import RestApiCallOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['test@test.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

# schedule_interval=timedelta(days=1)
# * * * * *
# 분 시 일 월 요일
dag = DAG(
    'AIRFLOW_TEST',
    default_args=default_args,
    description='AIRFLOW TEST',
    schedule_interval='0/10 * * * *'
)

command_t1 = "/home/hunetdb/jars/success.sh >> /home/hunetdb/jars/logs/success.log"
command_t2 = "/home/hunetdb/jars/error.sh >> /home/hunetdb/jars/logs/error.log"
command_t3 = "/home/hunetdb/jars/exception.sh >> /home/hunetdb/jars/logs/exception.log"


def get_start_info(context):
    context['ti'].xcom_push(key="first_info", value={
        "dag_id": context['ti'].dag_id,
        "hostname": str(context['ti'].hostname),
        "start_time": context['ti'].start_date.strftime("%Y-%m-%dT%H:%M:%S")
    })


def result_msg(context):
    kafka_msg_send = RestApiCallOperator(task_id="restapi_msg_send", endpoint="/kafka_api")
    kafka_msg_send.execute(context)


t1 = BashOperator(
    task_id='AIRFLOW_TEST_SUCCESS1',
    bash_command=command_t1,
    retries=0,
    on_failure_callback=result_msg,
    on_success_callback=get_start_info,
    dag=dag)

s1 = BashOperator(
    task_id='sleep1',
    depends_on_past=False,
    bash_command='sleep 5',
    retries=0,
    on_failure_callback=result_msg,
    dag=dag)

# t2 = BashOperator(
#     task_id='AIRFLOW_TEST_ERROR',
#     bash_command=command_t2,
#     retries=0,
#     on_failure_callback=result_msg,
#     dag=dag)

# s2 = BashOperator(
#     task_id='sleep2',
#     depends_on_past=False,
#     bash_command='sleep 5',
#     retries=0,
#     on_failure_callback=result_msg,
#     dag=dag)

# t3 = BashOperator(
#     task_id='AIRFLOW_TEST_EXCEPTION',
#     bash_command=command_t3,
#     retries=0,
#     on_failure_callback=result_msg,
#     dag=dag)

t4 = BashOperator(
    task_id='AIRFLOW_TEST_SUCCESS2',
    bash_command=command_t1,
    retries=0,
    on_failure_callback=result_msg,
    on_success_callback=result_msg,
    dag=dag)

s1.set_upstream(t1)
# t2.set_upstream(s1)
# s2.set_upstream(t2)
# t3.set_upstream(s2)
t4.set_upstream(s1)
