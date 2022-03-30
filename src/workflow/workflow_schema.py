from typing import List, Optional

from pydantic import BaseModel


class WorkflowStepSchema(BaseModel):
    stepName: str
    image: str
    dependsOn: List[str]
    command: Optional[List[str]]

    def __hash__(self):
        if self.command:
            return hash((self.stepName, self.image, tuple(self.dependsOn), tuple(self.command)))
        else:
            return hash((self.stepName, self.image, tuple(self.dependsOn)))


class WorkflowSchema(BaseModel):
    steps: List[WorkflowStepSchema]
