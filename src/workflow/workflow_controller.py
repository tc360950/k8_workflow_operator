from datetime import datetime
from typing import Dict, List, Tuple, Set, Optional

import kubernetes
import networkx

from src.workflow.constants import WorkflowConstants
from src.workflow.status import WorkflowStatusEnum
from src.workflow.workflow import Workflow
from src.workflow.workflow_schema import WorkflowStepSchema, WorkflowSchema


class WorkflowController:
    __WORKFLOW_EXECUTED_STEPS_ANNOTATION__ = "workflow-executed-steps"
    __WORKFLOW_STARTED_STEPS_ANNOTATION__ = "workflow-started-steps"
    __STEP_SEPARATOR__ = ';'
    __EMPTY_EXECUTED_STEPS_STRING__ = ''

    STEP_EXECUTED_SELECTOR = f'metadata.annotations.{__WORKFLOW_EXECUTED_STEPS_ANNOTATION__}'

    @staticmethod
    def validate_workflow_spec(workflow_body: Dict) -> Tuple[bool, str]:
        try:
            graph = Workflow(WorkflowSchema(steps=WorkflowController.get_workflow_steps(workflow_body)))
        except (RuntimeError, KeyError) as e:
            return False, str(e)

        if not networkx.is_directed_acyclic_graph(graph):
            return False, "Workflow contains a cycle!"
        return True, ""

    @staticmethod
    def patch_workflow(patch: Dict, workflow_name: str, namespace: str) -> None:
        kubernetes.client.CustomObjectsApi().patch_namespaced_custom_object(
            body=patch,
            name=workflow_name,
            namespace=namespace,
            group=WorkflowConstants.GROUP,
            version=WorkflowConstants.API_VERSION,
            plural=WorkflowConstants.PLURAL
        )

    @staticmethod
    def get_workflow_steps(workflow_body: Dict) -> List[WorkflowStepSchema]:
        return [WorkflowStepSchema(**x) for x in workflow_body['spec']['containers']]

    @staticmethod
    def get_executed_steps(workflow_body: Dict) -> List[str]:
        executed_value = workflow_body['metadata']['annotations'][
            WorkflowController.__WORKFLOW_EXECUTED_STEPS_ANNOTATION__]
        if executed_value == WorkflowController.__EMPTY_EXECUTED_STEPS_STRING__:
            return []
        return executed_value.split(WorkflowController.__STEP_SEPARATOR__)

    @staticmethod
    def has_finished(workflow_body: Dict) -> bool:
        return len(WorkflowController.get_workflow_steps(workflow_body)) == len(
            WorkflowController.get_executed_steps(workflow_body))

    @staticmethod
    def add_executed_step(workflow_body: Dict, patch: Dict, step_name: str) -> None:
        steps = list(set(WorkflowController.get_executed_steps(workflow_body)).union({step_name}))
        patch.setdefault("metadata", {}).setdefault("annotations", {})[
            WorkflowController.__WORKFLOW_EXECUTED_STEPS_ANNOTATION__] = WorkflowController.__STEP_SEPARATOR__.join(
            steps)

    @staticmethod
    def init_executed_steps(workflow_body: Dict) -> None:
        workflow_body.setdefault("metadata", {}).setdefault("annotations", {})[
            WorkflowController.__WORKFLOW_EXECUTED_STEPS_ANNOTATION__] \
            = WorkflowController.__EMPTY_EXECUTED_STEPS_STRING__
        workflow_body.setdefault("metadata", {}).setdefault("annotations", {})[
            WorkflowController.__WORKFLOW_STARTED_STEPS_ANNOTATION__] \
            = WorkflowController.__EMPTY_EXECUTED_STEPS_STRING__

    @staticmethod
    def add_to_started_steps(workflow_body: Dict, patch: Dict, new_started: List[str]) -> None:
        started = set(WorkflowController.__get_already_started_steps(workflow_body)).union(set(new_started))
        patch.setdefault("metadata", {}).setdefault("annotations", {})[
            WorkflowController.__WORKFLOW_STARTED_STEPS_ANNOTATION__] = \
            WorkflowController.__STEP_SEPARATOR__.join(started)

    @staticmethod
    def get_steps_to_execute(workflow_body, executed_steps: List[str]) -> Set[WorkflowStepSchema]:
        steps = Workflow(WorkflowSchema(steps=WorkflowController.get_workflow_steps(workflow_body))) \
            .get_next_to_execute(set(executed_steps))
        already_started = WorkflowController.__get_already_started_steps(workflow_body)
        return set([s for s in steps if s.stepName not in already_started])

    @staticmethod
    def update_status(workflow_body: Dict, status: WorkflowStatusEnum, message=Optional[str]) -> None:
        workflow_body['status'] = {
            'workflow-status': str(status),
            'status-changed': str(datetime.now()),
            'message': str(message)
        }

    @staticmethod
    def get_status(workflow_body: Dict) -> WorkflowStatusEnum:
        return WorkflowStatusEnum.from_string(workflow_body['status']['workflow-status'])

    @staticmethod
    def get_status_timestamp(workflow_body: Dict):
        return datetime.fromisoformat(workflow_body['status']['status-changed'])

    @staticmethod
    def get_max_step_timeout(workflow_body: Dict) -> int:
        return workflow_body['spec']['maxStepTimeout']

    @staticmethod
    def __get_already_started_steps(workflow_body: Dict) -> List[str]:
        in_progress = workflow_body['metadata']['annotations'][
            WorkflowController.__WORKFLOW_STARTED_STEPS_ANNOTATION__]
        if in_progress == WorkflowController.__EMPTY_EXECUTED_STEPS_STRING__:
            return []
        return in_progress.split(WorkflowController.__STEP_SEPARATOR__)
