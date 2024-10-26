import datetime
import airflow
from airflow.decorators import dag, task

from airflow.operators.python import PythonOperator
import logging
from time import sleep


@task.kubernetes(
    task_id="task-kubernetes",
)
def kubernetes_task_example():
    logging.info("This task runs using KubernetesExecutor")
    sleep(10)
    logging.info("Task completed")


@task.kubernetes(
    task_id="task-kubernetes-cluster",
    in_cluster=True,
)
def kubernetes_cluster_task_example():
    logging.info("This task runs using KubernetesExecutor")
    sleep(10)
    logging.info("Task completed")


@task.kubernetes(
    task_id="queue-task-kubernetes",
    queue="kubernetes",
)
def kubernetes_task_queue():
    logging.info("This task runs using KubernetesExecutor")
    sleep(10)
    logging.info("Task completed")


@task(
    task_id="queue-kubernetes",
    queue="kubernetes",
)
def kubernetes_example():
    logging.info("This task runs using KubernetesExecutor")
    sleep(10)
    logging.info("Task completed")


@task(
    task_id="task-celery",
)
def celery_example():
    logging.info("This task runs using CeleryExecutor")
    sleep(10)
    logging.info("Task completed")


@dag(
    "sample_celery_kubernetes",
    start_date=datetime.datetime(2022, 1, 1),
    schedule_interval=None,
)
def etl():
    kubernetes_example()
    kubernetes_task_example()
    kubernetes_task_queue()
    kubernetes_cluster_task_example()
    celery_example()


d = etl()


import logging
import traceback
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python import PythonOperator
from functools import wraps
from inspect import signature

# Параметры для логирования
MAX_BACKTRACE_LIMIT = 5
logger = logging.getLogger("airflow.task")

def trace_handler(frame, event, arg):
    if event != 'call':
        return
    
    # Фильтруем только вызовы, связанные с ElasticSearchTaskHandler
    if 'ElasticSearchTaskHandler' in frame.f_globals:
        code = frame.f_code
        func_name = code.co_name
        filename = code.co_filename

        # Получаем аргументы функции
        args_info = frame.f_locals
        arg_str = ', '.join([f"{k}={v}" for k, v in args_info.items()])

        # Логгируем функцию, аргументы и бектрейс
        logger.info(f"Method Call: {func_name} in {filename} with args: {arg_str}")
        logger.info("Traceback:")
        stack_trace = ''.join(traceback.format_stack(limit=MAX_BACKTRACE_LIMIT))
        logger.info(stack_trace)

def wrap_elastic_logging(handler_class):
    for name, method in handler_class.__dict__.items():
        if callable(method):
            @wraps(method)
            def wrapped_method(*args, **kwargs):
                logger.info(f"Calling method {name} with args: {args} and kwargs: {kwargs}")
                result = method(*args, **kwargs)
                return result
            setattr(handler_class, name, wrapped_method)

def log_elastic_methods():
    from airflow.providers.elasticsearch.log.es_task_handler import ElasticsearchTaskHandler
    wrap_elastic_logging(ElasticsearchTaskHandler)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='trace_elastic_logging_dag',
    default_args=default_args,
    description='Trace all calls to Elastic logging handler',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Таск для установки перехвата методов
    trace_task = PythonOperator(
        task_id='log_elastic_methods_task',
        python_callable=log_elastic_methods,
    )

trace_task

