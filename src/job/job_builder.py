from typing import Dict, List

from kubernetes import client


class BatchJobBuilder:
    RESTART_POLICY: str = "Never"
    JOB_KIND: str = "Job"
    API_VERSION: str = "batch/v1"

    def __init__(self, job_name: str):
        self.job_name = job_name
        self.container = []
        self.pod_spec = client.V1PodSpec(containers=[], restart_policy=BatchJobBuilder.RESTART_POLICY)
        self.metadata = client.V1ObjectMeta(name=job_name, labels={})

    def add_labels(self, labels: Dict[str, str]) -> 'BatchJobBuilder':
        self.metadata.labels.update(labels)
        return self

    def add_container(self, name: str, image: str, commands: List[str]) -> 'BatchJobBuilder':
        self.pod_spec.containers.append(client.V1Container(name=name, image=image, command=commands))
        return self

    def build(self, backoff_limit: int) -> client.V1Job:
        return client.V1Job(
                    api_version=BatchJobBuilder.API_VERSION,
                    kind=BatchJobBuilder.JOB_KIND,
                    metadata=self.metadata,
                    spec=client.V1JobSpec(backoff_limit=backoff_limit, template=client.V1PodTemplateSpec(spec=self.pod_spec)))
