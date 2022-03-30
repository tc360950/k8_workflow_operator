import subprocess
import time

import kubernetes.client.api
import pytest
from kopf.testing import KopfRunner

from src.job.job_controller import JobController
from src.workflow.constants import WorkflowConstants
from src.workflow.status import WorkflowStatusEnum
from src.workflow.workflow_controller import WorkflowController

TEST_NAMESPACE = "workflow-test7"


@pytest.fixture(autouse=True)
def run_before_and_after_tests():
    WorkflowConstants.BACKOFF_LIMIT = 1
    subprocess.run(f"kubectl create namespace {TEST_NAMESPACE}", shell=True, check=True)
    subprocess.run(f"kubectl apply -f ../crd/workflow.yaml -n {TEST_NAMESPACE}", shell=True, check=True)
    time.sleep(2)
    yield
    with KopfRunner(['run', '-A', '--verbose', '../workflow_operator.py']) as runner:
        subprocess.run(f"kubectl delete namespace {TEST_NAMESPACE}", shell=True, check=True)


def test_correct_job_creation_and_labeling():
    """
        Check that there's 1-1 correspondence between workflow step and a job.
        Check that jobs inherit labeling from the workflow.
    """
    with KopfRunner(['run', '-A', '--verbose', '../workflow_operator.py']) as runner:
        subprocess.run(f"kubectl apply -f ./data/simple_list_workflow.yaml -n {TEST_NAMESPACE}", shell=True, check=True)
        time.sleep(10)

    assert runner.exit_code == 0
    assert runner.exception is None

    jobs = kubernetes.client.api.BatchV1Api().list_namespaced_job(TEST_NAMESPACE).items
    jobs = [x.to_dict() for x in jobs]
    assert len(jobs) == 3
    for job in jobs:
        assert JobController.get_owning_workflow(job)['metadata']['name'] == 'simple-list-workflow'
        assert job['metadata']['labels']['label'] == 'test-label'

    steps = set([JobController.get_job_workflow_step_name(job) for job in jobs])
    assert steps == {"step0", "step1", "step2"}


def test_correct_cascading_deletion():
    """
        Check that deletion of workflow results in deletion of all connected jobs.
    """
    with KopfRunner(['run', '-A', '--verbose', '../workflow_operator.py']) as runner:
        subprocess.run(f"kubectl apply -f ./data/simple_list_workflow.yaml -n {TEST_NAMESPACE}", shell=True, check=True)
        time.sleep(10)
        subprocess.run(f"kubectl delete -f ./data/simple_list_workflow.yaml -n {TEST_NAMESPACE}", shell=True,
                       check=True)
        time.sleep(5)

    assert runner.exit_code == 0
    assert runner.exception is None

    jobs = kubernetes.client.api.BatchV1Api().list_namespaced_job(TEST_NAMESPACE).items
    jobs = [x.to_dict() for x in jobs]
    assert len(jobs) == 0


def test_workflow_failure():
    """
        The last step of workflow failing_list_workflow.yaml fails due to incorrect command.
        Check that jobs corresponding to all steps are created and that the workflow end up in Failed state.
    """
    with KopfRunner(['run', '-A', '--verbose', '../workflow_operator.py']) as runner:
        subprocess.run(f"kubectl apply -f ./data/failing_list_workflow.yaml -n {TEST_NAMESPACE}", shell=True,
                       check=True)
        time.sleep(30)

    assert runner.exit_code == 0
    assert runner.exception is None

    jobs = kubernetes.client.api.BatchV1Api().list_namespaced_job(TEST_NAMESPACE).items
    jobs = [x.to_dict() for x in jobs]
    assert len(jobs) == 3
    workflow = kubernetes.client.api.CustomObjectsApi().get_namespaced_custom_object(namespace=TEST_NAMESPACE,
                                                                                     name='failing-list-workflow',
                                                                                     group=WorkflowConstants.GROUP,
                                                                                     version=WorkflowConstants.API_VERSION,
                                                                                     plural=WorkflowConstants.PLURAL)
    assert WorkflowController.get_status(workflow) == WorkflowStatusEnum.FAILED


def test_correct_job_status():
    """
        Step step3 should not be started before step2 is completed (which takes 200 seconds).
        Check that's what happens and that the workflow completes eventually.
    """
    with KopfRunner(['run', '-A', '--verbose', '../workflow_operator.py']) as runner:
        subprocess.run(f"kubectl apply -f ./data/diamond_workflow.yaml -n {TEST_NAMESPACE}", shell=True, check=True)
        time.sleep(5)

        workflow = kubernetes.client.api.CustomObjectsApi().get_namespaced_custom_object(namespace=TEST_NAMESPACE,
                                                                                         name='diamond-workflow',
                                                                                         group=WorkflowConstants.GROUP,
                                                                                         version=WorkflowConstants.API_VERSION,
                                                                                         plural=WorkflowConstants.PLURAL)
        assert WorkflowController.get_status(workflow) == WorkflowStatusEnum.STARTED
        time.sleep(10)

        jobs = kubernetes.client.api.BatchV1Api().list_namespaced_job(TEST_NAMESPACE).items
        jobs = [x.to_dict() for x in jobs]
        # step3 should not be started yet!
        assert len(jobs) == 3

        time.sleep(80)
        workflow = kubernetes.client.api.CustomObjectsApi().get_namespaced_custom_object(namespace=TEST_NAMESPACE,
                                                                                         name='diamond-workflow',
                                                                                         group=WorkflowConstants.GROUP,
                                                                                         version=WorkflowConstants.API_VERSION,
                                                                                         plural=WorkflowConstants.PLURAL)
        assert WorkflowController.get_status(workflow) == WorkflowStatusEnum.COMPLETED


def test_workflow_timeout():
    """
        The workflow diamond_workflow_with_timeout.yaml has maxStepTimeout set to 60 but its step step2
        takes 200 seconds ot complete. The workflow should be put in Failed state by the daemon.
        Step step3 should never start!
    """
    with KopfRunner(['run', '-A', '--verbose', '../workflow_operator.py']) as runner:
        subprocess.run(f"kubectl apply -f ./data/diamond_workflow_with_timeout.yaml -n {TEST_NAMESPACE}", shell=True,
                       check=True)
        time.sleep(110)

        workflow = kubernetes.client.api.CustomObjectsApi().get_namespaced_custom_object(namespace=TEST_NAMESPACE,
                                                                                         name='diamond-workflow-tm',
                                                                                         group=WorkflowConstants.GROUP,
                                                                                         version=WorkflowConstants.API_VERSION,
                                                                                         plural=WorkflowConstants.PLURAL)
        assert WorkflowController.get_status(workflow) == WorkflowStatusEnum.FAILED
        time.sleep(200)

        workflow = kubernetes.client.api.CustomObjectsApi().get_namespaced_custom_object(namespace=TEST_NAMESPACE,
                                                                                         name='diamond-workflow-tm',
                                                                                         group=WorkflowConstants.GROUP,
                                                                                         version=WorkflowConstants.API_VERSION,
                                                                                         plural=WorkflowConstants.PLURAL)
        assert WorkflowController.get_status(workflow) == WorkflowStatusEnum.FAILED


def test_relabel():
    """
        Check that relabeling of workflow results in relabeling of corresponding jobs.
    """
    with KopfRunner(['run', '-A', '--verbose', '../workflow_operator.py']) as runner:
        subprocess.run(f"kubectl apply -f ./data/simple_list_workflow.yaml -n {TEST_NAMESPACE}", shell=True, check=True)
        time.sleep(10)
        subprocess.run(f"kubectl apply -f ./data/simple_list_workflow_relabel.yaml -n {TEST_NAMESPACE}", shell=True,
                       check=True)
        time.sleep(20)

    assert runner.exit_code == 0
    assert runner.exception is None

    jobs = kubernetes.client.api.BatchV1Api().list_namespaced_job(TEST_NAMESPACE).items
    jobs = [x.to_dict() for x in jobs]
    assert len(jobs) == 3
    for job in jobs:
        assert JobController.get_owning_workflow(job)['metadata']['name'] == 'simple-list-workflow'
        assert job['metadata']['labels']['label'] == 'test-label2'
        assert job['metadata']['labels']['label2'] == 'test-label2'

    steps = set([JobController.get_job_workflow_step_name(job) for job in jobs])
    assert steps == {"step0", "step1", "step2"}


def test_spec_update():
    """
     Here we relabel and update the spec of a workflow.
     Check that jobs corresponding to old spec are deleted.
    """
    with KopfRunner(['run', '-A', '--verbose', '../workflow_operator.py']) as runner:
        subprocess.run(f"kubectl apply -f ./data/simple_list_workflow.yaml -n {TEST_NAMESPACE}", shell=True, check=True)
        time.sleep(10)
        subprocess.run(f"kubectl apply -f ./data/simple_list_workflow_relabel_and_spec_update.yaml -n {TEST_NAMESPACE}", shell=True,
                       check=True)
        time.sleep(20)

    assert runner.exit_code == 0
    assert runner.exception is None

    jobs = kubernetes.client.api.BatchV1Api().list_namespaced_job(TEST_NAMESPACE).items
    jobs = [x.to_dict() for x in jobs]
    assert len(jobs) == 3
    for job in jobs:
        assert JobController.get_owning_workflow(job)['metadata']['name'] == 'simple-list-workflow'
        assert job['metadata']['labels']['label'] == 'test-label2'
        assert job['metadata']['labels']['label2'] == 'test-label2'

    steps = set([JobController.get_job_workflow_step_name(job) for job in jobs])
    assert steps == {"mstep0", "mstep1", "mstep2"}