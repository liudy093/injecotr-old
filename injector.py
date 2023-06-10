import json
import os
import subprocess
import time
from random import choices, uniform
import csv
from typing import Optional

import yaml
from grpc import insecure_channel
from shutil import copyfile

from sc_pb2 import InputWorkflowReply, InputWorkflowRequest
from sc_pb2_grpc import SchedulerControllerStub
import boto3
from botocore.exceptions import ClientError
import math

workflows_protobuf = []
workflows_json = []
csv_writer: Optional[csv.DictWriter] = None
csv_file = None

MAKE_WORKFLOW_NUM = int(os.environ.get("MAKE_WORKFLOW_NUM", "10000"))
TEST_WITH_ARGO = os.environ.get("TEST_WITH_ARGO", False) in (
    True,
    "True",
    "true",
    "yes",
)
TEST_WITH_SC = os.environ.get("TEST_WITH_SC", True) in (
    True,
    "True",
    "true",
    "yes",
)

# 发送性能指标写入文件
WRITE_METRICS_TO_FILE = os.environ.get("WRITE_METRICS_TO_FILE", True) in (
    True,
    "True",
    "true",
    "yes",
)

# 特殊工作流模式：no 表示不执行特殊模式。time 表示定制time属性为AB。cost表示定制cost属性为AB
# 特殊工作流模式下会生成两个相同工作流，但第一个为定制工作流，第二个为非定制工作流
SPECIAL_TEST = os.environ.get("SPECIAL_TEST", "no")
# 发送定制和非定制工作流之间时间间隔，单位：秒
# 默认10秒
# 可以使用小数秒，例如：1.2秒
SPECIAL_TEST_WAIT_SEC = float(os.environ.get("SPECIAL_TEST_WAIT_SEC", "10"))

# 发送性能指标写入SOS存储：METRIC_FILE_SOS_BUCKET，METRIC_FILE_SOS_AK，METRIC_FILE_SOS_SK均不为空
METRIC_FILE_SOS_BUCKET = os.environ.get("METRIC_FILE_SOS_BUCKET", "")
METRIC_FILE_SOS_URL = os.environ.get("METRIC_FILE_SOS_URL", "")
METRIC_FILE_SOS_AK = os.environ.get("METRIC_FILE_SOS_AK", "")
METRIC_FILE_SOS_SK = os.environ.get("METRIC_FILE_SOS_SK", "")
METRIC_FILE_SOS = (
    METRIC_FILE_SOS_BUCKET != "" and METRIC_FILE_SOS_AK != "" and METRIC_FILE_SOS_SK != ""
)
if METRIC_FILE_SOS:  # 如果要写入sos存储，必须先写入文件
    WRITE_METRICS_TO_FILE = True

SLEEP_SECS = int(os.environ.get("SLEEP_SECS", "1"))  # 发送等待间隔，秒
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))  # 每次发送的工作流个数
NEW_CORE_ADDRESS = os.environ.get("NEW_CORE_ADDRESS", "scheduler-controller-service:6060")
TEST_NUM = int(os.environ.get("TEST_NUM", "100"))  # 总测试轮数
ACTION_ON_FINISH = os.environ.get(
    "ACTION_ON_FINISH", "exit"
)  # 结束后的行为，“exit” 执行 TEST_NUM 轮测试后，退出；"sleep" 执行结束后休眠
CUSTOM_WF_RATE = float(os.environ.get("CUSTOM_WF_RATE", "0"))
# 控制器时间因子，单位：秒。
# 向控制器发送单一工作流时，需要延时多少时间
SC_TIME_FACTOR = float(os.environ.get("SC_TIME_FACTOR", "0"))
# 控制器时间因子区间终点
# 如果为0表示不使用随机区间
# 最终的随机区间为 [SC_TIME_FACTOR, SC_TIME_FACTOR_END]
# 如果SC_TIME_FACTOR_END不为0，必须大于SC_TIME_FACTOR的值
# 随机区间内的所有值被取到的概率相等
SC_TIME_FACTOR_END = float(os.environ.get("SC_TIME_FACTOR_END", "0"))
# argo 时间因子，单位：秒。
# 向argo发送单一工作流时，需要延时多少时间
ARGO_TIME_FACTOR = float(os.environ.get("ARGO_TIME_FACTOR", "0"))
ENABLE_DRAW_GRAPH = os.environ.get("ENABLE_DRAW_GRAPH", False) in (
    True,
    "True",
    "true",
    "yes",
)
# 任务的CPU占用率，0-100 整数
TASK_CPU_C = int(os.environ.get("TASK_CPU_C", "100"))
# 任务的MEM占用（MB）
TASK_MEM_C = int(os.environ.get("TASK_MEM_C", "64"))


def upload_file(file_name, bucket, object_name="metrics.csv"):
    try:
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=METRIC_FILE_SOS_AK,
            aws_secret_access_key=METRIC_FILE_SOS_SK,
            endpoint_url=METRIC_FILE_SOS_URL,
        )
        bucket = s3.Bucket(bucket)
        bucket.upload_file(Filename=file_name, Key=object_name)
    except ClientError as e:
        return False
    return True


def make_dags():
    import app

    # 检测如果存在 /tmp/dag/workflows.lock 不再生成新的工作流
    if os.path.exists("/tmp/dag/workflows.lock"):
        print("工作流已存在，不再生成新工作流")
        return

    app._gen_dags(
        count=MAKE_WORKFLOW_NUM,
        layernode=3,
        maxlayer=3,
        skiplayer=0,
        density=False,
        singlein=True,
        singleout=True,
        image="harbor.cloudcontrolsystems.cn/workflow/task:latest",
        outtype="json,protobuf",
        compress="snappy",
        duration_range="10,25",
        dest_path="/tmp",
        enable_graph=ENABLE_DRAW_GRAPH,
        custom_wf_rate=CUSTOM_WF_RATE,
        task_cpu=TASK_CPU_C,
        task_mem=TASK_MEM_C,
    )

    if ENABLE_DRAW_GRAPH:
        app._gen_graphs("/tmp/dag")

    # 写入特征文件
    with open("/tmp/dag/workflows.lock", "w") as f:
        f.write("gen locked")


def json_to_argo_workflow_yaml(json_str) -> str:
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
    return yaml.dump(yaml_data_struct)


def read_dags():
    global workflows_protobuf, workflows_json

    # 读入 dag json 文件，循环发送到控制器
    dag_path = "/tmp/dag"
    json_file_paths = []
    protobuf_file_paths = []
    with open(os.path.join(dag_path, "files.txt"), "r") as f:
        files = f.read()  # 每个文件：[type]:path 都是 json
        for fl in files.splitlines():
            typ, pth = fl.split(":")
            if typ == "json":
                json_file_paths.append(pth)
            elif typ == "protobuf":
                protobuf_file_paths.append(pth)

    for it_path in json_file_paths:
        with open(os.path.join(dag_path, it_path), "rb") as f:
            ct = f.read()
            workflows_json.append(ct)

    for it_path in protobuf_file_paths:
        with open(os.path.join(dag_path, it_path), "rb") as f:
            ct = f.read()
            workflows_protobuf.append(ct)


def send_to_argo(workflow) -> int:
    a1 = time.time_ns()
    with open("/tmp/123.yaml", "w") as f:
        f.write(json_to_argo_workflow_yaml(workflow))
    subprocess.run(["/bin/argo", "submit", "-n", "argo", "/tmp/123.yaml"])
    return time.time_ns() - a1


def send_to_sc(workflows) -> int:
    a1 = time.time_ns()
    channel = insecure_channel(NEW_CORE_ADDRESS)
    try:
        controller_grpc_client = SchedulerControllerStub(channel)
        reply: InputWorkflowReply = controller_grpc_client.InputWorkflow(
            InputWorkflowRequest(workflow=workflows)
        )
        print(f"send {len(workflows)} workflows to scheduler controller")
    finally:
        channel.close()
    return time.time_ns() - a1


def send_dags_x_per_y_seconds_to_sc(
    index_from: int, batch_idx: int, num: int, sleep_secs: int
) -> int:
    # 仅向控制器传送工作流
    data_set = choices(workflows_protobuf, k=num)
    elp_time = send_to_sc(data_set)
    factor = (
        uniform(SC_TIME_FACTOR, SC_TIME_FACTOR_END)
        if SC_TIME_FACTOR_END != 0
        else SC_TIME_FACTOR
    )
    avg_time = math.ceil(float(elp_time) / float(num) + factor * 1000000000)

    if WRITE_METRICS_TO_FILE:
        csv_writer.writerow(
            {
                "type": "core",
                "index": index_from,
                "batch_idx": batch_idx,
                "elapsed_time": avg_time,
            }
        )
        csv_file.flush()
        os.sync()
        print(f"core,{index_from},{batch_idx},{avg_time}")

    print(f"send to <Controller> and wait {sleep_secs} secs...")
    time.sleep(sleep_secs)
    return index_from + num


def send_dags_x_per_y_seconds_to_argo(
    index_from: int, batch_idx: int, num: int, sleep_secs: int
) -> int:
    # 仅向Argo传送工作流
    data_set = choices(workflows_json, k=num)
    total_time = 0.0
    for idx, d in enumerate(data_set):
        total_time += send_to_argo(d)
        print(f"send workflow {index_from+idx} to argo")
    avg_time = math.ceil(total_time / float(num) + ARGO_TIME_FACTOR * 1000000000)

    if WRITE_METRICS_TO_FILE:
        csv_writer.writerow(
            {
                "type": "argo",
                "index": index_from,
                "batch_idx": batch_idx,
                "elapsed_time": avg_time,
            }
        )
        csv_file.flush()
        os.sync()
        print(f"argo,{index_from},{batch_idx},{avg_time}")

    print(f"send to <Argo> and wait {sleep_secs} secs...")
    time.sleep(sleep_secs)
    return index_from + num


def special_test():
    from make_dag import make_one_dag
    from app import calc_func

    maxlayer = 3
    layernode = 3

    # 计算节点数量函数
    a, b, c = calc_func(0, 3, maxlayer, layernode / 2, layernode)
    # func = lambda x: 1 if layernode == 1 else lambda x: round(a * x * x + b * x + c)
    func = lambda x: round(a * x * x + b * x + c)

    make_one_dag(
        filename="/tmp/1",
        dag_name="1",
        maxlayer=maxlayer,
        skiplayer=0,
        singlein=False,
        singleout=True,
        density=False,
        image="harbor.cloudcontrolsystems.cn/workflow/task:latest",
        outtypes="protobuf",
        compress="snappy",
        duration_ranges=(10, 25),
        enable_graph=False,
        custom_wf_rate=0,
        func=func,
        special=SPECIAL_TEST,
        second_file="/tmp/2",
    )

    with open("/tmp/2.data.snappy", "rb") as f:
        ct = f.read()
        send_to_sc([ct])
        print(f"发送 定制工作流（{SPECIAL_TEST}）类型工作流到控制器")
    if SPECIAL_TEST_WAIT_SEC > 0:
        time.sleep(SPECIAL_TEST_WAIT_SEC)
    with open("/tmp/1.data.snappy", "rb") as f:
        ct = f.read()
        send_to_sc([ct])
        print(f"发送 非定制 类型工作流到控制器")


if __name__ == "__main__":
    if SPECIAL_TEST != "no":
        special_test()
    else:
        if WRITE_METRICS_TO_FILE:
            print("Save metric file to: /usr/local/dag/metrics.csv")
            csv_file = open("/usr/local/dag/metrics.csv", "w")
            csv_writer = csv.DictWriter(
                csv_file,
                fieldnames=["type", "index", "batch_idx", "elapsed_time"],
            )  # elapsed_time：发送工作流的耗时，单位为 ns 纳秒
            csv_writer.writeheader()

        make_dags()
        read_dags()

        sc_index_from = 0
        argo_index_from = 0

        for i in range(TEST_NUM):
            if TEST_WITH_SC:
                sc_index_from = send_dags_x_per_y_seconds_to_sc(
                    sc_index_from, i, BATCH_SIZE, SLEEP_SECS
                )
            if TEST_WITH_ARGO:
                argo_index_from = send_dags_x_per_y_seconds_to_argo(
                    argo_index_from, i, BATCH_SIZE, SLEEP_SECS
                )

        if METRIC_FILE_SOS:
            upload_file("/usr/local/dag/metrics.csv")

    if ACTION_ON_FINISH == "exit":
        exit(1)
    else:
        while True:
            time.sleep(60)
