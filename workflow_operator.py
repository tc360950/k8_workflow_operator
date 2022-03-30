from datetime import datetime
from typing import Set

import kopf
import kubernetes
from kubernetes import client

from src.job.job_controller import JobController
from src.workflow.status import WorkflowStatusEnum
from src.workflow.workflow_controller import WorkflowController
from src.workflow.workflow_schema import WorkflowStepSchema


@kopf.on.create('workflows')
def create_workflow(body, namespace, patch, logger, **kwargs):
    logger.info(f"Starting creation of workflow handler in namespace {namespace}...")
    is_valid, mess = WorkflowController.validate_workflow_spec(body)
    if not is_valid:
        WorkflowController.update_status(patch, WorkflowStatusEnum.FAILED, mess)
    else:
        WorkflowController.update_status(patch, WorkflowStatusEnum.CREATED)
        WorkflowController.init_executed_steps(patch)


@kopf.on.field('workflows', field=WorkflowController.STEP_EXECUTED_SELECTOR)
def update_workflow_after_step_execution(body, name, namespace, patch, logger, **kwargs):
    logger.info(f"Starting workflow step completion handler in namespace {namespace} for workflow {name}...")

    if WorkflowController.get_status(body) in [WorkflowStatusEnum.COMPLETED, WorkflowStatusEnum.FAILED]:
        logger.info(f"Workflow {name} is already in finished state. Ignoring request.")
        return

    if WorkflowController.has_finished(body):
        logger.info(f"Workflow {name} has executed all its steps.")
        WorkflowController.update_status(patch, WorkflowStatusEnum.COMPLETED)
    else:
        WorkflowController.update_status(patch, WorkflowStatusEnum.STARTED)
        steps_to_execute = WorkflowController.get_steps_to_execute(body,
                                                                   executed_steps=WorkflowController.get_executed_steps(
                                                                       body))
        logger.info(f"Workflow {name} will start execution of the following steps:\n{steps_to_execute}")
        start_workflow_steps(steps_to_execute, name, namespace, logger, body)
        WorkflowController.add_to_started_steps(body, patch, [s.stepName for s in steps_to_execute])


@kopf.on.event('jobs', labels=JobController.JOB_SELECTOR)
def handle_workflow_job_completion(event, namespace, logger, **kwargs):
    if event['type'] == 'MODIFIED':
        logger.info(
            f"Starting job event handler for job {event['object']['metadata']['name']} in namespace {namespace}...")

        workflow = JobController.get_owning_workflow(event['object'])
        step_name = JobController.get_job_workflow_step_name(event['object'])
        workflow_name = workflow['metadata']['name']
        patch = {}

        if JobController.has_completed(event['object']):
            logger.info(f"Job corresponding to step {step_name} in workflow {workflow_name} has completed.")
            WorkflowController.add_executed_step(workflow, patch, step_name)
        elif JobController.has_failed(event['object']):
            logger.info(f"Job corresponding to step {step_name} in workflow {workflow_name} has failed.")
            WorkflowController.update_status(patch, WorkflowStatusEnum.FAILED, f"Step {step_name} has failed.")

        WorkflowController.patch_workflow(patch=patch, workflow_name=workflow_name, namespace=namespace)


@kopf.on.update('workflows', field='metadata.labels')
def relabel(diff, name, namespace, logger, **kwargs):
    logger.info(f"Starting handler for relabeling of workflow {name} in namespace {namespace}...")
    labels_patch = {field[0]: new for op, field, old, new in diff}
    logger.info(f"Labels patch is: {labels_patch}")
    jobs = JobController.fetch_workflow_job_names(namespace, workflow_name=name)

    job_patch = {'metadata': {'labels': labels_patch}}
    for job_name in jobs:
        logger.info(f"Patching labels of job {job_name}...")
        JobController.patch_job(namespace, name=job_name, patch=job_patch)


@kopf.on.update('workflows', field='spec')
def spec_update(patch, body, name, namespace, logger, **kwargs):
    logger.info(f"Starting handler for update of workflow {name} spec field in namespace {namespace}...")

    is_valid, mess = WorkflowController.validate_workflow_spec(body)
    if not is_valid:
        WorkflowController.update_status(patch, WorkflowStatusEnum.FAILED, mess)
    else:
        WorkflowController.update_status(patch, WorkflowStatusEnum.CREATED, message="Restarted job after spec update")
        WorkflowController.init_executed_steps(patch)

        logger.info(f"Deleting jobs corresponding to old spec...")
        for job_name in JobController.fetch_workflow_job_names(namespace, name):
            kubernetes.client.api.BatchV1Api().delete_namespaced_job(name=job_name, namespace=namespace)


@kopf.daemon('workflows', initial_delay=30)
def monitor_workflow_timeout(stopped, name, body, logger, patch, **kwargs):
    if WorkflowController.get_max_step_timeout(body) == -1:
        return
    while not stopped:
        logger.info(f"Daemon for workflow {name} is awaken....")
        timeout = WorkflowController.get_max_step_timeout(body)
        ts_diff = (datetime.now().timestamp() - WorkflowController.get_status_timestamp(body).timestamp())
        if ts_diff > timeout:
            logger.info(f"Detected timeout for workflow {name} with difference {ts_diff}")
            WorkflowController.update_status(patch, WorkflowStatusEnum.FAILED, "Workflow timeout")
            break
        logger.info(f"Going to sleep with diff {ts_diff}...")
        stopped.wait(10)


def start_workflow_step(step: WorkflowStepSchema, workflow_name: str, namespace: str, workflow_body) -> None:
    job = JobController.create_job(step, workflow_name, workflow_body)
    kopf.append_owner_reference(job, workflow_body)
    client.BatchV1Api().create_namespaced_job(namespace=namespace, body=job)


def start_workflow_steps(steps: Set[WorkflowStepSchema], workflow_name: str, namespace: str, logger,
                         workflow_body) -> None:
    for step in steps:
        logger.info(f"Starting job for step {step.stepName} in workflow {workflow_name}...")
        start_workflow_step(step, workflow_name, namespace, workflow_body)
        logger.info(f"Job for step {step.stepName} in workflow {workflow_name} started successfully.")
