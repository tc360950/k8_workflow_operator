import itertools
from typing import List, Set

import networkx as nx

from src.workflow.workflow_schema import WorkflowStepSchema, WorkflowSchema

"""
    Represents DAG graph of workflow steps.  
"""


class Workflow(nx.DiGraph):
    def __init__(self, schema: WorkflowSchema):
        super().__init__()
        self.__name_to_node = {}
        self.__add_steps(schema.steps)

    def get_next_to_execute(self, executed_steps: Set[str]) -> Set[WorkflowStepSchema]:
        """
            Returns set of nodes representing steps which can be executed once steps from @executed_steps are executed.
            Node @n ends up in the result if:
                a) it is a direct descendant of a node from @executed_steps, and
                b) it is not in @executed_steps, and
                c) all its predecessors are in @executed_steps
        """
        if not executed_steps:
            return self.__get_nodes_without_predecessors()
        to_execute = set()
        # Iterate over direct descendants of executed steps
        for child in self.__get_successors([self.__name_to_node[s] for s in executed_steps]):
            if child.stepName in executed_steps:
                continue
            if all([n.stepName in executed_steps for n in self.predecessors(child)]):
                to_execute.add(child)

        return to_execute

    def __get_nodes_without_predecessors(self) -> Set[WorkflowStepSchema]:
        return set([step for step in list(self.nodes) if not list(self.predecessors(step))])

    def __get_successors(self, nodes: List[WorkflowStepSchema]) -> Set[WorkflowStepSchema]:
        """
            Returns set of successors of nodes in @nodes.
            A successor is a direct descendant of a node.
        """
        return set(itertools.chain(*[list(self.successors(n)) for n in nodes]))

    def __add_step(self, step: WorkflowStepSchema) -> None:
        self.add_node(step)
        self.__name_to_node[step.stepName] = step

    def __add_steps(self, steps: List[WorkflowStepSchema]) -> None:
        for step in steps:
            self.__add_step(step)
        self.__link_steps(steps)

    def __link_steps(self, steps: List[WorkflowStepSchema]) -> None:
        for step in steps:
            for parent in step.dependsOn:
                self.add_edge(self.__name_to_node[parent], step)
