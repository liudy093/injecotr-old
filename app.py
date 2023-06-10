import os
from collections import Counter
from pathlib import Path
from typing import List, Set

import click
import numpy as np
from numpy import mat
from tqdm import tqdm

from make_dag import make_one_dag, guess_filename


def calc_func(x1, y1, x2, y2, y3):
    # 算矩阵的Least squares
    x3 = (x1 + x2) / 2
    A = mat([[1, x1, x1 * x1], [1, x2, x2 * x2], [1, x3, x3 * x3]])
    Y = mat([[y1], [y2], [y3]])
    C = A.I * Y
    cx = C.tolist()
    a = cx[2][0]
    b = cx[1][0]
    c = cx[0][0]
    return a, b, c


@click.group()
def cli():
    pass


def _gen_graphs(dest_dir: str):
    if not os.path.exists(dest_dir):
        print(f"未找到指定目录: {dest_dir}")
        return

    graph_dest_dir = os.path.join(dest_dir, "graphs")
    if not os.path.exists(graph_dest_dir):
        os.mkdir(graph_dest_dir)

    for root, dirs, files in os.walk(dest_dir, False):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        files[:] = [
            f
            for f in files
            if not f.startswith(".") and os.path.splitext(f)[-1] == ".dot"
        ]

        rel_path = Path(root).relative_to(dest_dir)

        # 目录树的叶节点下才有dag文件
        # 去除 graphs 文件夹
        if dirs or ("graphs" in str(rel_path)):
            continue

        dst_dir = os.path.join(graph_dest_dir, rel_path)
        if not os.path.exists(dst_dir):
            print("创建路径：{} of root {} rel {}".format(dst_dir, root, rel_path))
            os.makedirs(dst_dir)

        # 调用dot命令画出来
        for f in files:
            src_filename = os.path.join(root, f)
            dst_filename = os.path.join(dst_dir, f.split(".")[0])
            os.system("dot -Kdot -Tsvg {0} -o {1}.svg".format(src_filename, dst_filename))
            print("绘制 {}.svg".format(dst_filename))


@click.command("graph")
@click.argument("dest_dir", type=click.STRING)
def gen_graphs(dest_dir: str):
    _gen_graphs(dest_dir)


def _gen_dags(
    count: int,
    layernode: int,
    maxlayer: int,
    skiplayer: int,
    density: bool,
    singlein: bool,
    singleout: bool,
    image: str,
    outtype: str,
    compress: str,
    duration_range: str,
    dest_path: str,
    enable_graph: bool,
    custom_wf_rate: float,
    task_cpu: int,
    task_mem: int
):
    outtypes = [c.strip() for c in outtype.split(",")]
    duration_ranges = tuple([int(c.strip()) for c in duration_range.split(",")])

    # dest_path 存放DAG的目标路径。此路径下会新建dag目录，所有文件均存在dag目录下
    dest_dir = os.path.join(dest_path, "dag")
    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir)

    # 计算节点数量函数
    a, b, c = calc_func(0, 3, maxlayer, layernode / 2, layernode)
    # func = lambda x: 1 if layernode == 1 else lambda x: round(a * x * x + b * x + c)
    func = lambda x: round(a * x * x + b * x + c)

    max_layer_num = 0
    min_layer_num = maxlayer
    max_layer_width = 0
    max_node_num = 0
    edge_densitys = []
    df_sizes = []
    fl, sl, tl = 0, 0, 0
    customization_wf_count = 0
    with open(os.path.join(dest_dir, "files.txt"), "w") as list_file:
        for i in tqdm(range(count)):
            # 三级目录结构，第一级100，第二级1000，第三级1000
            # 第三级存放具体的DAG文件，
            # DAG文件包括：index.json
            if tl == 1000:
                sl += 1
                tl = 0
            if sl == 1000:
                fl += 1
                sl = 0
            if fl == 100:
                raise Exception("DAG数量超过上限")

            relative_path = os.path.join(str(fl), str(sl))
            file_path = os.path.join(dest_dir, relative_path)
            if not os.path.exists(file_path):
                os.makedirs(file_path)

            layer_num, node_num_per_layer, ed, df_size, customization = make_one_dag(
                os.path.join(file_path, str(i)),
                str(i),
                maxlayer,
                skiplayer,
                singlein,
                singleout,
                density,
                image,
                outtypes,
                compress,
                duration_ranges,
                enable_graph,
                custom_wf_rate,
                func,
                task_cpu=task_cpu,
                task_mem=task_mem
            )

            max_layer_num = max(max_layer_num, layer_num)
            min_layer_num = min(min_layer_num, layer_num)
            max_layer_width = max(max_layer_width, max(node_num_per_layer))
            max_node_num = max(max_node_num, sum(node_num_per_layer))
            edge_densitys.append(round(ed, 4))
            df_sizes.append(df_size)
            if customization:
                customization_wf_count += 1

            for data_type in outtypes:
                list_file.write(
                    f"{data_type}:{guess_filename(os.path.join(relative_path, str(i)), data_type, compress )}\n"
                )

    print("========= 统计信息 =========")
    print("共生成 {} 个DAG，其中：".format(count))
    print("\t最大深度: {}".format(max_layer_num))
    print("\t最小深度: {}".format(min_layer_num))
    print("\t最大宽度: {}".format(max_layer_width))
    print("\t单DAG最大节点数: {}".format(max_node_num))
    print("\t定制工作流数量：{} 个".format(customization_wf_count))
    # print("边密度分布: {}".format(Counter(edge_densitys)))
    print("输出数据文件(DataFile)统计：")
    print("\t长度最大值：{} Bytes".format(max(df_sizes)))
    print("\t长度最小值：{} Bytes".format(min(df_sizes)))
    print("\t长度值标准差：{} Bytes".format(round(np.std(df_sizes), 2)))
    print("\t长度均值：{} Bytes".format(np.mean(df_sizes)))
    print("\t长度中位数：{} Bytes".format(np.median(df_sizes)))
    print("\t长度众数：{} Bytes".format(np.argmax(np.bincount(df_sizes))))


@click.command("dag")
@click.option("--count", type=click.INT, default=5, help="需要生成的DAG数量")
@click.option("--layernode", type=click.INT, default=10, help="每层node的最大数量")
@click.option("--maxlayer", type=click.INT, default=100, help="最大层数")
@click.option("--skiplayer", type=click.INT, default=5, help="允许存在跳层的最大层数(间隔几层连接)")
@click.option(
    "--density", type=click.BOOL, default=False, help="密集图（除了第一层，其余层内每个节点至少有一个前导）"
)
@click.option("--singlein", type=click.BOOL, default=False, help="保证单入口DAG图")
@click.option("--singleout", type=click.BOOL, default=True, help="保证单出口DAG图")
@click.option(
    "--image",
    type=click.STRING,
    default="liudy093/task:latest",
    help="Docker 镜像",
)
@click.option(
    "--outtype",
    type=click.STRING,
    default="json",
    help="数据输出类型: json,protobuf 可设置多个，逗号分隔",
)
@click.option(
    "--compress", type=click.STRING, default="none", help="输出压缩类型：none, gzip, snappy"
)
@click.option(
    "--duration_range",
    type=click.STRING,
    default="30,600",
    help="每个节点运行持续时间范围：min,max 闭区间",
)
@click.option(
    "--enable_graph",
    type=click.BOOL,
    default=False,
    help="是否输出dot。如果不输出dot文件，之后将不能使用绘图功能输出",
)
@click.option(
    "--custom_wf_rate",
    type=click.FLOAT,
    default=0,
    help="定制工作流占比(取值范围[0-1])，如果为0则禁用定制工作流，为1则全部都是定制工作流。默认为0(禁用)",
)
@click.option(
    "--task_cpu",
    type=click.INT,
    default=0,
    help="任务cpu占用百分比。取值范围[0,100]。",
)
@click.option(
    "--task_mem",
    type=click.INT,
    default=0,
    help="任务内存占用量，单位：MB",
)
@click.argument("dest_path", type=click.STRING)
def gen_dags(
    count: int,
    layernode: int,
    maxlayer: int,
    skiplayer: int,
    density: bool,
    singlein: bool,
    singleout: bool,
    image: str,
    outtype: str,
    compress: str,
    duration_range: str,
    dest_path: str,
    enable_graph: bool,
    custom_wf_rate: float,
    task_cpu: int,
    task_mem: int
):
    _gen_dags(
        count,
        layernode,
        maxlayer,
        skiplayer,
        density,
        singlein,
        singleout,
        image,
        outtype,
        compress,
        duration_range,
        dest_path,
        enable_graph,
        custom_wf_rate,
        task_cpu,
        task_mem
    )


cli.add_command(gen_dags)
cli.add_command(gen_graphs)

if __name__ == "__main__":
    cli()
