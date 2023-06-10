import json
def json_to_argo_workflow_yaml(json_str) :
    dag = json.loads(json_str)
    tasks = []
    for typl in dag["topology"]:
        tasks.append(
            {
                "name": typl["name"],
                "template": "task",
                "dependencies": typl["dependencies"],
            }
        )
    yaml_data_struct = {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "Workflow",
        "metadata": {"generateName": "argo-test-wf-"},
        "spec": {
            "entrypoint": dag["workflow_name"],
            "podGC": {"strategy": "OnPodSuccess"},
            "ttlStrategy": {"secondsAfterCompletion": 60},
            "templates": [
                {
                    "name": "task",
                    "container": {
                        "image": "harbor.cloudcontrolsystems.cn/workflow/task:latest",
                        "imagePullPolicy": "IfNotPresent",  # default: Always(if latest tag is specified)
                        "resources": {
                            "limits": {"cpu": "2000m", "memory": "128Mi"},
                            "requests": {"cpu": "1000m", "memory": "64Mi"},
                        },
                    },
                },
                {"name": dag["workflow_name"], "dag": {"tasks": tasks}},
            ],
        },
    }