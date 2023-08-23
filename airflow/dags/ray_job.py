import asyncio
import logging
from uuid import uuid4

from ray.job_submission import JobSubmissionClient

from airflow.operators.python import get_current_context
from airflow.dags.config import GTAAS_GIT_DIR, JOB_STATUS_TO_DAG_STATUS, GTAAS_GIT_TAG
from airflow.dags.exception import RayJobFailedException


class RayJobSubmitProcess:
    def __init__(self, cli=None):
        self.client = None
        self.cli = cli

    @property
    def ray_address(self):
        context = get_current_context()
        return context["ti"].xcom_pull(key='ray_cluster_address', task_ids='generate_ray_address')

    @property
    def ray_runtime_env(self):
        return {
            "working_dir": GTAAS_GIT_DIR,
        }

    def get_submission_id(self, context):
        request_id = context["dag_run"].conf.get("request_id", str(uuid4()))
        task_id = context["ti"].task_id
        submission_id = f'{request_id}_{task_id}'

        if not self.is_submission_id_valid(submission_id):
            retry_count = context["ti"].try_number
            submission_id += f'_retry_{retry_count}'

        return submission_id

    def get_cli(self, context) -> str:
        """
        사용 예시
        >>> input_root = context["dag_run"].conf.get("input_root")
        >>> output_root = context["dag_run"].conf.get("output_root")
        >>> return f"flask bot video extract_frames --input-root '{input_root}' --output-root '{output_root}'"
        """
        return self.cli

    def wait_and_logging(self, submission_id):
        asyncio.run(self._async_wait_and_logging(submission_id))

    async def _async_wait_and_logging(self, submission_id):
        async for log_lines in self.client.tail_job_logs(submission_id):
            print(f':::Ray::: {submission_id} batch log: ...\n{log_lines}', end='')

    def is_submission_id_valid(self, submission_id):
        try:
            if self.client.get_job_info(submission_id):
                logging.info(f":::Ray::: Job {submission_id} already exists")
                return False
        except RuntimeError:
            return True

    def handle_job_result(self, job_info):
        context = get_current_context()
        context["ti"].xcom_push(key='job_info', value=job_info.dict())

        dag_status = JOB_STATUS_TO_DAG_STATUS[job_info.status]
        xcom_push_return_value = {"status": dag_status, "error_type": job_info.error_type}
        context["ti"].xcom_push(key='return_value', value=xcom_push_return_value)

        if dag_status == 'failure':
            error_message = job_info.message
            raise RayJobFailedException(error_message)

    def execute(self, **context):
        self.client = JobSubmissionClient(address=self.ray_address)

        cli = self.get_cli(context)
        submission_id = self.get_submission_id(context)

        logging.info(f":::Ray::: Submitting job {submission_id} to {self.ray_address} "
                     f"with gtaas tag: {GTAAS_GIT_TAG}, CLI: {cli}")
        submission_id = self.client.submit_job(
            submission_id=submission_id,
            entrypoint=cli,
            runtime_env=self.ray_runtime_env
        )

        self.wait_and_logging(submission_id)

        job_info = self.client.get_job_info(submission_id)
        logging.info(f":::Ray::: Job {submission_id} finished with status {job_info.status}")

        self.handle_job_result(job_info)
