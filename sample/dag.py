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
