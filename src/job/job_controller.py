import uuid
from typing import Dict, List, Optional

import kopf
import kubernetes

from src.job.job_builder import BatchJobBuilder
from src.workflow.constants import WorkflowConstants
from src.workflow.workflow_schema import WorkflowStepSchema


class JobController:
    __OWNING_WORKFLOW_NAME_LABEL__ = "kopf__workflow__kopf"
    __CORRESPONDING_WORKFLOW_STEP_LABEL__ = "kopf__workflow__step__kopf"
    JOB_SELECTOR = {__OWNING_WORKFLOW_NAME_LABEL__: kopf.PRESENT}

    @staticmethod
    def has_failed(job: Dict) -> bool:
        return 'conditions' in job['status'] and any(
            [c['type'] == 'Failed' and c['status'] == 'True' for c in job['status']['conditions']])

    @staticmethod
    def has_completed(job: Dict) -> bool:
        return 'conditions' in job['status'] and any(
            [c['type'] == 'Complete' and c['status'] == 'True' for c in job['status']['conditions']])

    @staticmethod
    def get_owning_workflow(job: Dict) -> Dict:
        return kubernetes.client.CustomObjectsApi().get_namespaced_custom_object(
            namespace=job['metadata']['namespace'],
            group=WorkflowConstants.GROUP,
            version=WorkflowConstants.API_VERSION,
            plural=WorkflowConstants.PLURAL,
            name=JobController.__get_job_workflow_name(job))

    @staticmethod
    def get_job_workflow_step_name(job: Dict) -> str:
        return job['metadata']['labels'][JobController.__CORRESPONDING_WORKFLOW_STEP_LABEL__]

    @staticmethod
    def create_job(step: WorkflowStepSchema, workflow_name: str, workflow_body: Dict) -> kubernetes.client.V1Job:
        job_name = step.stepName + '-' + str(uuid.uuid4())
        return BatchJobBuilder(job_name) \
            .add_container(job_name, step.image, commands=step.command) \
            .add_labels(JobController.__create_job_labels(workflow_name, step.stepName)) \
            .add_labels(workflow_body['metadata']['labels']) \
            .build(WorkflowConstants.BACKOFF_LIMIT)

    @staticmethod
    def fetch_workflow_job_names(namespace: str, workflow_name: str) -> List[str]:
        jobs = kubernetes.client.api.BatchV1Api().list_namespaced_job(namespace=namespace)
        jobs = [x.to_dict() for x in jobs.items if JobController.__get_job_workflow_name(x.to_dict()) == workflow_name]
        return [x['metadata']['name'] for x in jobs]

    @staticmethod
    def patch_job(namespace: str, patch: Dict, name: str) -> None:
        kubernetes.client.BatchV1Api().patch_namespaced_job(
            name=name,
            namespace=namespace,
            body=patch
        )

    @staticmethod
    def __create_job_labels(workflow_name: str, step_name: str) -> Dict:
        return {
            JobController.__OWNING_WORKFLOW_NAME_LABEL__: workflow_name,
            JobController.__CORRESPONDING_WORKFLOW_STEP_LABEL__: step_name
        }

    @staticmethod
    def __get_job_workflow_name(job: Dict) -> Optional[str]:
        return job['metadata']['labels'].get(JobController.__OWNING_WORKFLOW_NAME_LABEL__, None)