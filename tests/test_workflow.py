from src.workflow.workflow import Workflow
from src.workflow.workflow_schema import WorkflowStepSchema, WorkflowSchema

list_workflow = [
    WorkflowStepSchema(stepName="step0", image="", dependsOn=[]),
    WorkflowStepSchema(stepName="step1", image="", dependsOn=["step0"]),
    WorkflowStepSchema(stepName="step2", image="", dependsOn=["step1"]),
    WorkflowStepSchema(stepName="step3", image="", dependsOn=["step2"]),
    WorkflowStepSchema(stepName="step4", image="", dependsOn=["step3"]),
]

binary_tree_workflow = [
    WorkflowStepSchema(stepName="step0", image="", dependsOn=[]),
    WorkflowStepSchema(stepName="step1", image="", dependsOn=["step0"]),
    WorkflowStepSchema(stepName="step2", image="", dependsOn=["step0"]),
    WorkflowStepSchema(stepName="step3", image="", dependsOn=["step1"]),
    WorkflowStepSchema(stepName="step4", image="", dependsOn=["step1"]),
    WorkflowStepSchema(stepName="step5", image="", dependsOn=["step2"]),
    WorkflowStepSchema(stepName="step6", image="", dependsOn=["step2"]),
]

diamond_workflow = [
    WorkflowStepSchema(stepName="step0", image="", dependsOn=[]),
    WorkflowStepSchema(stepName="step1", image="", dependsOn=["step0"]),
    WorkflowStepSchema(stepName="step2", image="", dependsOn=["step0"]),
    WorkflowStepSchema(stepName="step3", image="", dependsOn=["step1", "step2"])
]


def test_list_workflow():
    workflow_graph = Workflow(WorkflowSchema(steps=list_workflow))
    assert workflow_graph.get_next_to_execute(set([])) == {list_workflow[0]}
    for i in range(1, len(list_workflow)):
        assert workflow_graph.get_next_to_execute(set([x.stepName for x in list_workflow[:i]])) == {list_workflow[i]}


def test_binary_tree_workflow():
    workflow_graph = Workflow(WorkflowSchema(steps=binary_tree_workflow))
    assert workflow_graph.get_next_to_execute(set()) == {binary_tree_workflow[0]}

    to_execute = workflow_graph.get_next_to_execute(set())
    counter = 1
    while to_execute:
        assert len(to_execute) == counter
        to_execute = workflow_graph.get_next_to_execute(set([x.stepName for x in to_execute]))
        counter = 2 * counter


def test_diamond_workflow():
    workflow_graph = Workflow(WorkflowSchema(steps=diamond_workflow))
    assert workflow_graph.get_next_to_execute(set()) == {diamond_workflow[0]}

    # Step3 can't be executed because it's waiting for step2
    assert (workflow_graph.get_next_to_execute({"step0", "step1"}) == {diamond_workflow[2]})
    assert (workflow_graph.get_next_to_execute({"step1", "step2"}) == {diamond_workflow[3]})