from datetime import datetime
from uuid import uuid4

from airflow.dags.ray_job import RayJobSubmitProcess


def generate_ray_job_root(**context):
    now = datetime.today().strftime("%Y-%m-%d")
    request_id = context["dag_run"].conf.get("request_id", str(uuid4()))
    ray_job_root = f'/mnt/files/airflow/{now}_{request_id}'
    ray_job_submit_process = RayJobSubmitProcess()
    ray_job_submit_process.cli = f'mkdir -p "{ray_job_root}"'
    ray_job_submit_process.execute(**context)
    context['ti'].xcom_push(key='ray_job_root', value=ray_job_root)


def delete_ray_job_root(**context):
    ray_job_root = context["ti"].xcom_pull(key='ray_job_root', task_ids='generate_ray_job_root')
    del_ray_job_cli = f'rm -rf "{ray_job_root}"'

    ray_job_submit_process = RayJobSubmitProcess()
    ray_job_submit_process.cli = del_ray_job_cli
    ray_job_submit_process.execute(**context)