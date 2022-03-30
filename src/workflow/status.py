from enum import Enum


class WorkflowStatusEnum(Enum):
    # At least one workflow step has been started
    STARTED = "Started"
    # No steps have been started
    CREATED = "Created"
    # Workflow failed - usually due to unsuccessful completion of batch job corresponding to a step
    FAILED = "Failed"
    # All workflow steps have been completed successfully
    COMPLETED = "Completed"

    def __str__(self):
        return self.value

    @staticmethod
    def from_string(s: str) -> 'WorkflowStatusEnum':
        for enum in WorkflowStatusEnum:
            if str(enum) == s:
                return enum
        raise ValueError(f"Could convert {s} to WorkflowStatusEnum!")

