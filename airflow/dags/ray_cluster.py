import logging
from dataclasses import dataclass

from airflow.operators.python import get_current_context
from ray.dashboard.modules.job.sdk import JobSubmissionClient

from airflow.dags.config import RAY_ADDRESS_FORMAT


@dataclass
class AksRayClusterAddress:
    RAY_CLUSTER_1: str = "ray-cluster-1"
    RAY_CLUSTER_2: str = "ray-cluster-2"

    @classmethod
    def to_list(cls):
        return list(cls().__dict__.values())


class RayClusterAddressGenerator:
    def __init__(self):
        self.ray_cluster_list = AksRayClusterAddress.to_list()

    def get_job_list(self, cluster_name):
        ray_address = RAY_ADDRESS_FORMAT.format(ray_cluster=cluster_name)
        job_list = JobSubmissionClient(ray_address).list_jobs()
        return job_list

    def get_num_running_jobs(self, cluster_name):
        job_list = self.get_job_list(cluster_name)
        running_jobs = [job for job in job_list if not job.status.is_terminal()]

        return len(running_jobs)

    def choose_ray_cluster(self, ray_cluster_list: list):
        return min(ray_cluster_list, key=self.get_num_running_jobs)

    def execute(self):
        context = get_current_context()
        ray_cluster_name = self.choose_ray_cluster(self.ray_cluster_list)
        aks_ray_address = RAY_ADDRESS_FORMAT.format(ray_cluster=ray_cluster_name)
        logging.info(f":::RAY::: Ray cluster address to use {ray_cluster_name}: {aks_ray_address}")

        context["ti"].xcom_push(key='ray_cluster_address', value=aks_ray_address)
