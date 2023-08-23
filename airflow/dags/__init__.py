from datetime import timedelta

from kubernetes.client import models as k8s

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'schedule_interval': None,
}

VOLUMES = [
    k8s.V1Volume(
        name=VOLUME_NAME,
        azure_file=k8s.V1AzureFileVolumeSource(
            secret_name=f'{VOLUME_NAME}-secret',
            share_name=VOLUME_NAME
        )
    )
]

VOLUME_MOUNTS = [
    k8s.V1VolumeMount(
        name=VOLUME_NAME,
        mount_path=f'/{VOLUME_NAME}'
    )
]

SMALL_K8S_RESOURCE = k8s.V1ResourceRequirements(
    requests={"memory": "256Mi", "cpu": "512m"},
    limits={"memory": "512Mi", "cpu": "512m"}
)

KUBE_EXEC_CONFIG_SMALL_RESOURCE = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    resources=SMALL_K8S_RESOURCE,
                )
            ],
        )
    )
}

LARGE_K8S_RESOURCE = k8s.V1ResourceRequirements(
    requests={"memory": "1000Mi", "cpu": "4000m"},
    limits={"memory": "2500Mi", "cpu": "4000m"}
)

KUBE_EXEC_CONFIG_LARGE_RESOURCE = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    resources=LARGE_K8S_RESOURCE,
                )
            ],
        )
    )
}


class GTaasProcessingOperator(KubernetesPodOperator):
    def __init__(self, **kwargs):
        super().__init__(
            namespace=NAMESPACE,
            volumes=VOLUMES,
            volume_mounts=VOLUME_MOUNTS,
            is_delete_operator_pod=True,
            env_vars=ENV_VARS,
            get_logs=True,
            startup_timeout_seconds=240,
            image=GTAAS_PROCESSING_OPERATOR_IMAGE,
            **kwargs
        )
