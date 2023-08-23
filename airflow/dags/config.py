from airflow.models.variable import Variable

from ray.dashboard.modules.job.common import JobStatus



GTAAS_PROCESSING_OPERATOR_IMAGE = f"<IMAGE>:<GIT_TAG>"

NAMESPACE = Variable.get('NAMESPACE')
VOLUME_NAME = Variable.get('VOLUME_NAME')
ENV_VARS = {
    'SENTRY_DSN': Variable.get('SENTRY_DSN'),
    'PASSWORD': Variable.get('PASSWORD'),
    'GSA_SLACKBOT_TOKEN': Variable.get('GSA_SLACKBOT_TOKEN')
}

GTAAS_GIT_TAG = 'ray_tag'  # Config.gtaas_processing_git_tag
GTAAS_GIT_DIR = f"https://<GIT_ACCESS_USER>:<GIT_ACCESS_TOKEN>@<GITHUB REPOSITORY URL>/archive/refs/tags/{GTAAS_GIT_TAG}.zip"

RAY_ADDRESS_FORMAT = "http://raycluster-autoscaler-head-svc.{ray_cluster}.svc:8265"

JOB_STATUS_TO_DAG_STATUS = {
    JobStatus.SUCCEEDED: 'success',
    JobStatus.FAILED: 'failure',
    JobStatus.STOPPED: 'failure',
    JobStatus.PENDING: 'pending',
    JobStatus.RUNNING: 'running',
}
