# About 
Custom Kubernetes operator (written using Kopf framework) for execution of workflows.
Workflow is a DAG of jobs which should be executed in the topological ordering of the graph (i.e. an edge A -> B means that 
job A must be executed successfully before job B is started).
# Local deployment
Install kopf:
``` pip install kopf[full-auth] ```

Install Kind https://github.com/kubernetes-sigs/kind

Create kind cluster: ``` kind create cluster``` (usually kind will be found in ~/go/bin/kind) 

Install Kopf on the cluster:

```
kubectl apply -f https://github.com/nolar/kopf/raw/main/peering.yaml

kubectl apply -f https://github.com/nolar/kopf/raw/main/examples/crd.yaml
```

Run the operator:

``` 
kopf run workflow_operator.py
```
# Creating Workflows

To create a workflow - create custom object of kind Workflow. 
Corresponding CRD can be found in *./crd/workflow.yaml* with schema:
``` 
spec:
  type: object
  properties:
    maxStepTimeout:
      type: integer
      default: 60 
    containers:
      type: array
      items:
        type: object
        properties:
          stepName:
            type: string
          image:
            type: string
          command:
            nullable: true
            type: array
            items:
              type: string
          dependsOn:
            type: array
            items:
              type: string
```

Where *maxStepTimeout* defines how many seconds to wait before a step (and thus the whole workflow) is considered to be failed.
Set this field to *-1* to allow unlimited step execution. 

Each container from *spec.containers* corresponds to one Kubernetes job. *dependsOn* is a list of names of steps which should be finished before execution of the
step is started.

Each step is assumed to be idempotent 

Each created workflow has a status field {"workflow-status" : Started | Created | Completed | Failed, "status-changed": Timestamp, "message": str}

# Tests 
You'll need a kubernetes cluster (Kind is recommended) to run the tests locally.
Apart from that, the tests are vanilla pytest tests.

# Events and responses 
The following is a short description of operator's reconciliation loop:
1. Workflow created -> Set status to STARTED and list of executed & started steps to []
2. Workflow's list of executed steps changed -> 

    if in FAILED status:\
        *ignore*\
    if all steps have been executed:\
        *update status to COMPLETED* \
    if any new steps can be submitted: \
        *Run jobs corresponding to new steps & update list of started steps*\
        *Set workflow's status to STARTED*
3. Job event -> 

    if job completed successfully:
        *update owning workflow's list of executed steps*
    if job failed: 
        *set owning workflow's status to FAILED*
    else:
        *ignore*
4. Workflow relabeling -> cascade changes to corresponding jobs 
5. Workflow deletion -> cascade deletion to corresponding jobs 
6. Workflow spec update ->\
    delete jobs corresponding to old spec\
    set workflow status to STARTED \
    set list of executed & started steps to []