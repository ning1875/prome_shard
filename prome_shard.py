import json
import logging
import os
import yaml
import time

from multiprocessing import Process, Queue, Pool, Manager
from consul_work import Consul
from get_targets import GetTarget
from consistent_hash_ring import ConsistentHashRing
from metrics import *
from ansi_new import run_play_with_res
import multiprocessing_logging

multiprocessing_logging.install_mp_handler()
logging.basicConfig(
    format='%(asctime)s %(levelname)s %(filename)s [func:%(funcName)s] [line:%(lineno)d]:%(message)s',
    datefmt="%Y-%m-%d %H:%M:%S",
    level="INFO"
)
logger = logging.getLogger()

# default vars
DEFAULT_YAML = "copy_file_and_reload_prome.yaml"
DEFAULT_PORT = "9090"
DEFAULT_JSON_FILE = "file_sd_by_prome_shared.json"


def load_base_config(yaml_path):
    with open(yaml_path, encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    return config


def shard_job(service_name, service_hash_map, config):
    start = time.perf_counter()
    has_func = hasattr(GetTarget, service_name)
    if not has_func:
        logging.error("[reflect_error][func:{}not_found in GetTarget]".format(
            service_name,
        ))
        return
    func = getattr(GetTarget, service_name)
    hash_ring = service_hash_map.get(service_name)
    if not hash_ring:
        logging.error("[hash_ring_error][func:{} not_found in service_hash_map]".format(
            service_name,
        ))
        return

    targets = func()

    end = time.perf_counter()
    if not targets:
        logging.error("[run_func_error_GetTarget.{}]".format(
            service_name,
        ))
        return
    M_GET_TARGET_RESULT_NUM.labels(service_name=service_name).set(len(targets))
    M_GET_TARGET_RESULT_TIME.labels(service_name=service_name).set(end - start)

    if len(targets) == 0:
        logging.error("[run_func_Get_empty_Target.{}]".format(
            service_name,
        ))
        return
    logging.info("[shard_job_start_for:{}]".format(
        service_name,

    ))
    node_map = {}
    for x in hash_ring.nodes:
        node_map[x] = []
    for index, i in enumerate(targets):
        target_addr_str = "_".join(i.get("targets"))
        target_node = hash_ring.get_node(str(target_addr_str))

        node_map[target_node].append(i)
    send_target_to_node(service_name, node_map, config)


def send_target_to_node(service_name, node_map, config):
    all_num = len(node_map)
    if all_num == 0:
        return

    shard_config_dic = config.get("shard_service")
    print(shard_config_dic)
    num = 1
    all_args = []

    for node, t_l in node_map.items():
        target_num = len(t_l)
        if target_num == 0:
            continue

        service_config_dic = shard_config_dic.get(service_name)
        if not service_config_dic:
            continue
        json_file_name = "{}_{}_{}_{}.json".format(
            service_name,
            node,
            "{}-{}".format(num, all_num),
            target_num
        )
        msg = "[gen_node_target][service_name:{}][node:{} shard:{}/{}][target_num:{}][json_file_name:{}]".format(
            service_name,
            node,
            num,
            all_num,
            target_num,
            json_file_name,
        )

        logging.info(msg)
        single_arg = [
            json_file_name,
            service_config_dic.get("dest_sd_file_name", DEFAULT_JSON_FILE),
            node,
            str(service_config_dic.get("port", DEFAULT_PORT)),
            service_config_dic.get("yaml_path", DEFAULT_YAML),

        ]
        # write_json_file

        with open(json_file_name, 'w') as f:
            f.write(json.dumps(t_l))
        all_args.append(single_arg)
        num += 1
    if len(all_args) == 0:
        logging.error("[send_target_to_node_zero_targets][service_name:{}]".format(service_name))
        return

    # with Pool(5) as p:
    #     res = p.starmap(ansible_send_work, all_args)
    #  daemon子进程不能在通过multiprocessing创建后代进程，否则当父进程退出后，它终结其daemon子进程，那孙子进程就成了孤儿进程了。当尝试这么做时，会报错：AssertionError: daemonic processes are not allowed to have children
    #

    for i in all_args:
        # scp_send_work(i)
        ansible_send_work(i)


def ansible_send_work(args):
    start = time.perf_counter()
    if not isinstance(args, list):
        return
    src_sd_file_name = args[0]
    dest_sd_file_name = args[1]
    ip = args[2]
    service_port = args[3]
    yaml_path = args[4]

    msg = "[ansible_send_work_args][src_sd_file_name:{}][dest_sd_file_name:{}][ip:{}][service_port:{}][yaml_path:{}]".format(
        src_sd_file_name,
        dest_sd_file_name,
        ip,
        service_port,
        yaml_path,

    )
    logging.info(msg)
    extra_vars = {
        "src_sd_file_name": src_sd_file_name,
        "dest_sd_file_name": dest_sd_file_name,
        "service_port": service_port,

    }
    res = run_play_with_res([ip], yaml_path, extra_vars=extra_vars)
    logging.info(
        "[ansible_send_work_result] [yaml_path:{}][ip:{}] [res:{}]".format(yaml_path, ip, json.dumps(res)))
    end = time.perf_counter()
    M_ANSIBLE_TIME.labels(ip=ip, src_sd_file_name=src_sd_file_name, dest_sd_file_name=dest_sd_file_name,
                          service_port=service_port, yaml_path=yaml_path).set(end - start)


def scp_send_work(args):
    start = time.perf_counter()
    if not isinstance(args, list):
        return
    src_sd_file_name = args[0]
    dest_sd_file_name = args[1]
    ip = args[2]
    service_port = args[3]
    yaml_path = args[4]

    cmd = "scp  -o StrictHostKeyChecking=no {} {}:/App/prometheus/sd/{}".format(src_sd_file_name, ip, dest_sd_file_name)
    os.popen(cmd)

    end = time.perf_counter()

    msg = "[scp_send_work][src_sd_file_name:{}][dest_sd_file_name:{}][ip:{}][service_port:{}][yaml_path:{}][time_took:{}]".format(
        src_sd_file_name,
        dest_sd_file_name,
        ip,
        service_port,
        yaml_path,
        end - start
    )
    logging.info(msg)
    M_ANSIBLE_TIME.labels(ip=ip, src_sd_file_name=src_sd_file_name, dest_sd_file_name=dest_sd_file_name,
                          service_port=service_port, yaml_path=yaml_path).set(end - start)


def consumer(sync_dq, service_hash_map, config):
    """
    消费节点变化信息的函数
    1.重新生成一致性哈希环
    2.相当于收敛
    :param sync_dq:
    :return:
    """
    msg = "[start service consumer..[pid:{}]".format(os.getpid())
    logger.info(msg)
    while True:
        msg = sync_dq.get()
        if msg:
            log_line = "[接收到任务][msg:{}]".format(msg)
            logger.info(log_line)
            shard_job(msg, service_hash_map, config)


def run_sync_targets(service_hash_map, config):
    while True:
        services = service_hash_map.keys()
        logging.info("[main_loop_run....][service_num:{}][detail:]".format(
            len(services),
            ",".join(services)
        ))

        for s in services:
            # 同步的方法
            # shard_job(s, service_hash_map, config)
            p = Process(target=shard_job, args=(s, service_hash_map, config))
            p.start()
            p.join(timeout=3)

        time.sleep(int(config.get("job_setting").get("ticker_interval", 60)))


def run(yaml_path):
    config = load_base_config(yaml_path)
    shard_service_d = config.get("shard_service")

    logging.info("[start_for_service][service_num:{}][detail:{}]".format(
        len(shard_service_d.keys()),
        ",".join(shard_service_d.keys()),

    ))
    # 1.创建一个Manger对象
    manager = Manager()

    # 2. 创建一个 全局一致性哈希map
    service_hash_map = manager.dict()
    # 同步的队列，用来通知是哪个采集服务节点发生收敛了
    sync_q = Queue()

    # consul
    consul_addr = config.get("consul").get("host")
    consul_port = int(config.get("consul").get("port"))
    consul_obj = Consul(consul_addr, consul_port)

    # step_1 注册服务 && 初始化hash-map
    # 获取consul所有服务

    all_service = consul_obj.get_all_service()
    for service_name, ii in shard_service_d.items():
        nodes = ii.get("nodes")
        port = ii.get("port")

        for host in nodes:
            one_service_id = "{}_{}_{}".format(service_name, host, port)
            this_service = all_service.get(one_service_id)
            if not this_service:
                # 说明服务不存在，需要注册

                res = consul_obj.register_service(
                    service_name, host, int(port)
                )
                logging.info("[new_service_need_register][register_service_res:{}][service:{}][node:{},port:{}]".format(
                    res, service_name, host, port
                ))
        # 给新注册的服务探测时间
        time.sleep(1)
        alive_nodes = consul_obj.get_service_health_node(service_name)

        ring = ConsistentHashRing(1000, alive_nodes)
        service_hash_map[service_name] = ring

        M_SHARD_SERVICE_DES.labels(service_name=service_name, service_port=port).set(1)

    # step_2 开启watch变化结果队列消费进程
    p_consumer = Process(target=consumer, args=(sync_q, service_hash_map, config))
    p_consumer.start()

    # step_3 开启consul watch 进程
    for service_name in shard_service_d.keys():
        p = Process(target=consul_obj.watch_service, args=(service_name, service_hash_map, sync_q))
        p.start()

    # step_4 开启metrics server统计线程
    # 但是这个库是线程模式，在多进程中不work
    start_http_server(int(config.get("http").get("port")))
    logging.info("[start_metrics_server:{}]".format(port))
    # step_5 主进程：开启定时同步target并发往采集器进程

    run_sync_targets(service_hash_map, config)


if __name__ == '__main__':
    cf = "config.yaml"

    run(cf)
