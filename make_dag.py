import json
from collections import defaultdict
from random import choice, choices, randint, shuffle, random
from typing import List, Optional, Tuple
import uuid

import cramjam
from graphviz import Digraph

import wf_pb2


def guess_filename(fname: str, data_type: str, compress_type: str) -> str:
    if data_type == "json":
        r = f"{fname}.json"
    elif data_type == "protobuf":
        r = f"{fname}.data"
        if compress_type != "none":
            r = f"{r}.{compress_type}"
    elif data_type == "config":
        r = f"{fname}.config.json"
    else:
        raise Exception(f"未知输出数据类型: {data_type}")
    return r


def make_one_dag(
    filename: str,
    dag_name: str,
    maxlayer: int,
    skiplayer: int,
    singlein: bool,
    singleout: bool,
    density: bool,
    image: str,
    outtypes: List[str],
    compress: str,
    duration_ranges: Tuple[int, int],
    enable_graph: bool,
    custom_wf_rate: float,
    func,
    special: str = "no",
    second_file: Optional[str] = None,
    task_cpu: int = 100,
    task_mem: int = 64,
):
    """
    @return 层数，每层节点数
    """
    dot = Digraph(comment=f"DAG {dag_name}")
    dot.attr(rankdir="TB")
    dot.attr("node", shape="circle")

    node_num_per_layer = []  # 每层节点数
    # 邻接矩阵，用于消除不连通子图
    # 不在邻接矩阵里的点，出度=0
    connection_map = defaultdict(list)
    # 逆邻接矩阵，key为节点，value为节点对应的前导节点，用于消除不连通子图, 建立密度图
    # 不在逆邻接矩阵里的点，入度=0
    reversed_connection_map = defaultdict(list)

    total_edge_count = 0

    # 计算层数
    # 至少3层
    if maxlayer < 3:
        print(f"警告：maxlayer参数期望设置为{maxlayer}，但允许的最小值为3，已自动调整为3")
    layer_count = randint(3, max(3, maxlayer))

    def __make_edge(src, dst):
        nonlocal total_edge_count

        dot.edge(f"t{src}", f"t{dst}")
        connection_map[src].append(dst)
        reversed_connection_map[dst].append(src)
        total_edge_count = total_edge_count + 1

    # 生成一个序列
    # [[1,2,3],[4,5],[6,7,8],[9]]
    layers: List[List[int]] = []
    if singlein:
        layers.append([0])
        next_node_index = 1
        start_layer_index = 1
        node_num_per_layer.append(1)
    else:
        next_node_index = 0
        start_layer_index = 0
    for lyr in range(start_layer_index, layer_count):
        nc = randint(1, func(lyr))
        this_layer = list(range(next_node_index, next_node_index + nc))
        next_node_index += nc

        node_num_per_layer.append(nc)

        # 随机重排
        shuffle(this_layer)

        # 写入dot文件
        for n in this_layer:
            dot.node(f"t{n}")

        layers.append(this_layer)

    # 生成边
    # 前一层的每个node都向下层随机连接
    for idx, this_layer in enumerate(layers[:-1]):
        for node in this_layer:
            target_node = choice(layers[idx + 1])
            # 建立边
            __make_edge(node, target_node)

    # 超过四层且skiplayer大于1的图，随机选择1~skiplayer层（可重复取样）做深层连接
    # 在选中的层内，30%的概率不做任何操作，以50%的概率向下探两层，20%的概率向下探三层
    if layer_count >= 4 and skiplayer > 1:
        candidate_layers = layers[:-3]
        deep_connection_layer_count = randint(1, skiplayer)
        for i in range(deep_connection_layer_count):
            c_layer_index = randint(0, len(candidate_layers) - 1)
            deep_count = choices([0, 2, 3], [0.3, 0.5, 0.2])[0]
            if deep_count == 0:
                continue
            src_node = choice(layers[c_layer_index])
            dst_node = choice(layers[c_layer_index + deep_count])
            # 建立边
            __make_edge(src_node, dst_node)

    # 处理最后一层，防止出现孤点
    for node in layers[-1]:
        if node not in reversed_connection_map:
            src_node = choice(layers[-2])
            # 建立边
            __make_edge(src_node, node)

    # 密度图
    if density:
        for idx, this_layer in enumerate(layers[1:-1]):
            for node in this_layer:
                if node not in reversed_connection_map:
                    # idx 在 layers 少一层
                    src_node = choice(layers[idx])
                    # 建立边
                    __make_edge(src_node, node)
    else:
        # 非密度图可能会存在不连通子图，需要纠正
        # 所有终结点必须从起点（第一层所有点）可达，否则终结点就属于不连通子图
        orphan_point = []  # 孤点
        terminal_node = []  # 终点

        # 首先视所有出度=0的点为孤点
        for this_layer in layers:
            for node in this_layer:
                if node not in connection_map.keys():
                    orphan_point.append(node)

        # 区分孤点和终点
        def __check_path(nodes):
            for n in nodes:
                if n in orphan_point:
                    orphan_point.remove(n)
                    terminal_node.append(n)
                else:
                    __check_path(connection_map[n])

        __check_path(layers[0])

        # 去除伪孤点，伪孤点重新视为终点
        def __get_top_nodes(node):
            if node not in reversed_connection_map:
                return [node]
            else:
                tops = []
                for nx in reversed_connection_map[node]:
                    rts = __get_top_nodes(nx)
                    tops.extend(rts)
                return tops

        def __has_path_to_terminal_node(n):
            if n in terminal_node:
                return True
            else:
                return any(__has_path_to_terminal_node(ny) for ny in connection_map[n])

        for op in orphan_point:
            top_nodes = __get_top_nodes(op)
            if any(__has_path_to_terminal_node(tn) for tn in top_nodes):
                orphan_point.remove(op)
                terminal_node.append(op)

        # 通过消除所有孤点，消除不连通子图
        orphan_node = []
        for op in orphan_point:
            orphan_node.extend(__get_top_nodes(op))
        for idx, this_layer in enumerate(layers[1:]):  # 孤点不可能在顶层
            for node in this_layer:
                if node in orphan_node:
                    src_node = choice(layers[idx])
                    # 建立边
                    __make_edge(src_node, node)

    # 单出口
    if singleout and len(layers[-1]) > 1:
        last_node_id = sum(node_num_per_layer)
        dot.node(f"t{last_node_id}")
        layers.append([last_node_id])
        for node in layers[-2]:
            __make_edge(node, last_node_id)

    if enable_graph:
        with open(f"{filename}.dot", "w") as f:
            f.write(dot.source)

    # 本工作流是否为定制工作流
    customization = False

    data_file_size = 0

    # 生成输出
    if "json" in outtypes:
        # 生成json文件
        total_duration = 0
        json_struct = []
        for this_layer in layers:
            layer_max_duration = 0
            for node in this_layer:
                dependencies = []
                if node in reversed_connection_map:
                    dependencies = reversed_connection_map[node]
                duration = randint(duration_ranges[0], duration_ranges[1])
                layer_max_duration = max(duration, layer_max_duration)
                json_struct.append(
                    {
                        "name": f"t{node}",
                        "dependencies": [f"t{d}" for d in dependencies],
                        "template": image,
                        "duration": duration,
                        "phase": "None",
                        "node_info": "None",
                        "cpu": randint(1, 4),
                        "mem": randint(512 * 1024 * 1024, 1024 * 1024 * 1024),
                        "env": {
                            "CPU_CONSUME": str(task_cpu),
                            "MEMORY_CONSUME": str(task_mem),
                        },
                    }
                )
            total_duration += layer_max_duration
        with open(guess_filename(filename, "json", compress), "w") as f:
            j = json.dumps(
                {
                    "workflow_name": "NoName",
                    "style": "Normal",
                    "custom_id": str(uuid.uuid4()),
                    "topology": json_struct,
                }
            )
            data_file_size = len(j)
            f.write(j)

    if "protobuf" in outtypes:
        wf = wf_pb2.Workflow()
        wf.workflow_name = "NoName"
        wf.custom_id = str(uuid.uuid4())
        wf.style = "Normal"

        # 如果是特殊模式的工作流，第一个一定是非定制
        if special != "no":
            print("[特殊类型] 设定 custom_wf_rate 为0")
            custom_wf_rate = 0

        # 按比例生成定制和非定制工作流
        r = random()
        if r < custom_wf_rate:
            wf.customization = True
            seq = {"A": ["C", "D"], "B": ["C", "D"], "C": ["A", "B"], "D": ["A", "B"]}
            chd = choice(list(seq.keys()))
            wf.cost_grade = chd
            wf.time_grade = choice(seq[chd])
            customization = True
        else:
            wf.customization = special != "no"
            wf.time_grade = ""
            wf.cost_grade = ""

        for this_layer in layers:
            for node in this_layer:
                wf_node = wf.topology.add()
                wf_node.name = f"t{node}"
                if node in reversed_connection_map:
                    dependencies = reversed_connection_map[node]
                    wf_node.dependencies.extend([f"t{d}" for d in dependencies])
                wf_node.duration = randint(duration_ranges[0], duration_ranges[1])
                wf_node.template = image
                wf_node.phase = "None"
                wf_node.node_info = "None"
                wf_node.cpu = 1
                wf_node.mem = randint(task_mem * 1024 * 1024, 2 * task_mem * 1024 * 1024)
                wf_node.env["CPU_CONSUME"] = str(task_cpu)
                wf_node.env["MEMORY_CONSUME"] = str(task_mem)
        with open(guess_filename(filename, "protobuf", compress), "wb") as fpb:
            d = wf.SerializeToString()
            if compress == "snappy":
                d = cramjam.snappy.compress(d)
            elif compress == "gzip":
                d = cramjam.gzip.compress(d)
            data_file_size = len(d)
            fpb.write(d)

        # 如果是特殊模式的工作流，多生成一个定制工作流
        if special != "no":
            if special == "time":
                wf.customization = True
                wf.cost_grade = "C"
                wf.time_grade = "A"
                customization = True
            elif special == "cost":
                wf.customization = True
                wf.cost_grade = "A"
                wf.time_grade = "C"
            with open(guess_filename(second_file, "protobuf", compress), "wb") as fpb:
                d = wf.SerializeToString()
                if compress == "snappy":
                    d = cramjam.snappy.compress(d)
                elif compress == "gzip":
                    d = cramjam.gzip.compress(d)
                fpb.write(d)
                print(f"[特殊类型] 写入 {compress} 压缩类型的 {special} 定制工作流")

    # 计算理论上最大边数
    pre_layer_node_count = 0
    max_edge_count = 0
    for nn in node_num_per_layer:
        max_edge_count += pre_layer_node_count * nn
        pre_layer_node_count = nn

    # 计算所有层所包含的节点总数
    total_node_count = sum(node_num_per_layer)

    # 输出此DAG的配置信息
    with open(guess_filename(filename, "config", ""), "w") as f:
        j = json.dumps(
            {
                "layer_count": layer_count,
                "node_count": total_node_count,
                "data_file_size": data_file_size,
            }
        )
        f.write(j)

    return (
        layer_count,
        node_num_per_layer,
        total_edge_count / max_edge_count,  # 边密度
        data_file_size,
        customization,
    )
