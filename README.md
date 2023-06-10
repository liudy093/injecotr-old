# 生成gRPC的python文件

python -m grpc.tools.protoc -I. --python_out=. --grpc_python_out=. sc.proto
python -m grpc.tools.protoc -I. --python_out=. --grpc_python_out=. wf.proto

# 运行 injector 的 docker 镜像, 需要测试argo的情况下
需要挂载 kubconfig 文件：
docker run -d -v <kubeconfig>:/kubeconfig harbor.cloudcontrolsystems.cn/workflow/injector:latest

# 运行argo和控制器对比测试，生成工作流需要保存的情况
docker run -d -v <kubeconfig_file>:/root/.kube/config:ro -v <workflow_saved_dir>:/tmp/dag:rw harbor.cloudcontrolsystems.cn/workflow/injector:latest

kubeconfig_file 如果放在 /root/kubeconfig_file, workflow_saved_dir 如果放在 /root/dag，则运行命令为：docker run -d -v /root/kubeconfig_file:/root/.kube/config:ro -v /root/dag:/tmp/dag:rw harbor.cloudcontrolsystems.cn/workflow/injector:latest

# 打包
docker build -t harbor.cloudcontrolsystems.cn/workflow/injector:latest .

# 支持的环境变量
| 环境变量               | 默认值                              | 解释                                                         |
| ---------------------- | ----------------------------------- | ------------------------------------------------------------ |
| MAKE_WORKFLOW_NUM      | "10000"                             | 生成多少工作流                                               |
| TEST_WITH_ARGO         | "False"                             | 测试向argo注入工作流                                         |
| TEST_WITH_SC           | "True"                              | 测试向控制器注入工作流                                       |
| WRITE_METRICS_TO_FILE  | "True"                              | 发送性能指标写入文件                                         |
| SPECIAL_TEST           | "no"                                | 特殊工作流模式：no 表示不执行特殊模式。time 表示定制time属性为AB。cost表示定制cost属性为AB。特殊工作流模式下会生成两个相同工作流，但第一个为定制工作流，第二个为非定制工作流 |
| SPECIAL_TEST_WAIT_SEC  | "10"                                | 发送定制和非定制工作流之间时间间隔，单位：秒。可以使用小数秒，例如：1.2秒 |
| METRIC_FILE_SOS_BUCKET | ""                                  | 发送性能指标写入SOS存储：METRIC_FILE_SOS_BUCKET，METRIC_FILE_SOS_AK，METRIC_FILE_SOS_SK均不为空 |
| METRIC_FILE_SOS_URL    | ""                                  | SOS存储地址                                                  |
| METRIC_FILE_SOS_AK     | ""                                  | SOS存储Access Key                                            |
| METRIC_FILE_SOS_SK     | ""                                  | SOS存储Security Key                                          |
| SLEEP_SECS             | "1"                                 | 发送等待间隔。单位：秒                                       |
| BATCH_SIZE             | "1000"                              | 每轮（批）次发送的工作流个数。！！必须小于MAKE_WORKFLOW_NUM值！！ |
| NEW_CORE_ADDRESS       | "scheduler-controller-service:6060" | 调度器控制器地址                                             |
| TEST_NUM               | "100"                               | 总测试轮数                                                   |
| ACTION_ON_FINISH       | "exit"                              | 结束后的行为。“exit”表示执行 TEST_NUM 轮测试后，退出；"sleep"表示执行 TEST_NUM 轮测试后休眠。如果需要读取metrics文件，要设为"sleep" |
| CUSTOM_WF_RATE         | "0"                               | 批量生成工作流时，定制工作流占比。取值范围：[0,1)            |
| SC_TIME_FACTOR         | "0"                                 | 控制器时间因子，单位：秒。向控制器发送一个工作流的实际时间基础上增加多少秒。支持小数时间，例如 1.2 秒 |
| SC_TIME_FACTOR_END     | "0"                                 | 控制器时间因子区间终点。如果为0表示不使用随机区间。非零值最终的随机区间为 [SC_TIME_FACTOR, SC_TIME_FACTOR_END]。随机区间内的所有值被取到的概率相等。 |
| ARGO_TIME_FACTOR       | "0"                                 | argo 时间因子，单位：秒。向argo发送一个工作流的实际时间基础上增加多少秒。 |
|ENABLE_DRAW_GRAPH| "False" | 是否画图。如果启用画图，在 dag/graph 下输出svg格式图像。启用画图会拖慢生成工作流的速度，请谨慎使用。 |
