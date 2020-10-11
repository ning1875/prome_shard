# import atfork
# 
# atfork.monkeypatch_os_fork_functions()
# from atfork import stdlib_fixer
# 
# stdlib_fixer.fix_logging_module()

import json
import logging
import os

import threadpool
import yaml
# import asyncio
import time

# from multiprocessing import Pool, Queue
# from multiprocessing.pool import ThreadPool
# from multiprocessing import Process
from collections import deque
from consul_work import start_thread_loop, watch_service, Consul
from get_targets import GetTarget
from threading import Thread
from consistent_hash_ring import ConsistentHashRing
from metrics import *
from ansi_new import run_play

# res = run_play(instances, yaml_path, extra_vars=extra_vars)
# logging.info("[handle_callback] [instances:{}] [res:{}]".format(",".join(instances), json.dumps(res)))

logging.basicConfig(
    # TODO console 日志,上线时删掉
    # filename=LOG_PATH,
    format='%(asctime)s %(levelname)s %(filename)s %(funcName)s [line:%(lineno)d]:%(message)s',
    datefmt="%Y-%m-%d %H:%M:%S",
    level="INFO"
)
# global vars
SERVICE_HASH_MAP = {}
ALL_CONFIG_DIC = {}

# default vars
DEFAULT_YAML = "copy_file_and_reload_prome.yaml"
DEFAULT_PORT = "9090"
DEFAULT_JSON_FILE = "file_sd_by_prome_shared.json"


def load_base_config(yaml_path):
    with open(yaml_path, encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    return config


def shard_job(service_name):
    start = time.clock()
    func = getattr(GetTarget, service_name)
    if not func:
        logging.error("[reflect_error][func:{}not_found in GetTarget]".format(
            service_name,
        ))
        return

    hash_ring = SERVICE_HASH_MAP.get(service_name)
    if not hash_ring:
        logging.error("[hash_ring_error][func:{} not_found in SERVICE_HASH_MAP]".format(
            service_name,
        ))
        return

    targets = func()

    end = time.clock()
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
    send_target_to_node(service_name, node_map)


def send_target_to_node(service_name, node_map):
    all_num = len(node_map)
    if all_num == 0:
        return

    shard_config_dic = ALL_CONFIG_DIC.get("shard_service")
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
            # "./{}".format(service_config_dic.get("yaml_path", DEFAULT_YAML)),
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

    # multi thread  run

    # pool = ThreadPool(len(all_args))
    # pool.map(ansible_send_work, all_args)
    #
    # pool.close()
    # pool.join()

    for i in all_args:
        # t = Thread(target=ansible_send_work, kwargs={"args": i})
        # t.setDaemon(True)
        # t.start()
        # ansible_send_work(i)
        scp_send_work(i)

    # for i in all_args:
    #     process = Process(target=ansible_send_work, kwargs={'args': i})
    #     process.start()
    #     process.join()


def ansible_send_work(args):
    start = time.clock()
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
    res = run_play([ip], yaml_path, extra_vars=extra_vars)
    logging.info(
        "[ansible_send_work_result] [yaml_path:{}][ip:{}] [res:{}]".format(yaml_path, ip, json.dumps(res)))
    end = time.clock()
    M_ANSIBLE_TIME.labels(ip=ip, src_sd_file_name=src_sd_file_name, dest_sd_file_name=dest_sd_file_name,
                          service_port=service_port, yaml_path=yaml_path).set(end - start)


def scp_send_work(args):
    start = time.clock()
    if not isinstance(args, list):
        return
    src_sd_file_name = args[0]
    dest_sd_file_name = args[1]
    ip = args[2]
    service_port = args[3]
    yaml_path = args[4]

    cmd = "scp  -o StrictHostKeyChecking=no {} {}:/App/prometheus/sd/{}".format(src_sd_file_name, ip, dest_sd_file_name)
    os.popen(cmd)

    end = time.clock()

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


def consumer(sync_dq):
    while True:
        if sync_dq:
            msg = sync_dq.popleft()
            if msg:
                shard_job(msg)


def run_sync_target():
    while True:
        services = SERVICE_HASH_MAP.keys()

        for i in services:
            shard_job(i)

        time.sleep(int(ALL_CONFIG_DIC.get("job_setting").get("ticker_interval", 600)))


def run_sync_target_thread():
    while True:
        services = SERVICE_HASH_MAP.keys()

        task_pool = threadpool.ThreadPool(20)
        rets = threadpool.makeRequests(shard_job, services)
        [task_pool.putRequest(req) for req in rets]
        task_pool.wait()

        logging.info("[main_loo_run....][service_num:{}][detail:]".format(
            len(services),
            ",".join(services)
        ))
        # pool = ThreadPool(len(services))
        # pool.map(shard_job, services)
        # 
        # pool.close()
        # pool.join()

        time.sleep(int(ALL_CONFIG_DIC.get("job_setting").get("ticker_interval", 600)))


def run(yaml_path):
    config = load_base_config(yaml_path)
    global ALL_CONFIG_DIC
    ALL_CONFIG_DIC = config
    shard_service_d = config.get("shard_service")

    logging.info("[start_for_service][service_num:{}][detail:{}]".format(
        len(shard_service_d.keys()),
        ",".join(shard_service_d.keys()),

    ))
    # queue

    sync_dq = deque()

    # consul
    consul_addr = config.get("consul").get("host")
    consul_port = int(config.get("consul").get("port"))
    consul_obj = Consul(consul_addr, consul_port)

    # 注册服务 && 初始化hash-map
    for service_name, ii in shard_service_d.items():
        nodes = ii.get("nodes")
        port = ii.get("port")

        ring = ConsistentHashRing(1000, nodes)
        SERVICE_HASH_MAP[service_name] = ring
        for host in nodes:
            res = consul_obj.register_service(
                service_name, host, int(port)
            )
            logging.info("[register_service_res:{}][service:{}][node:{},port:{}]".format(
                res, service_name, host, port
            ))

        M_SHARD_SERVICE_DES.labels(service_name=service_name, service_port=port).set(1)

    # 开启watch变化结果队列消费线程
    consumer_thread = Thread(target=consumer, kwargs={'sync_dq': sync_dq})
    consumer_thread.setDaemon(True)
    consumer_thread.start()

    # 开启consul watch
    for service_name in shard_service_d.keys():
        t = Thread(target=consul_obj.block_get_health, args=(service_name, SERVICE_HASH_MAP, sync_dq))
        t.setDaemon(True)
        t.start()

    # metrics server
    start_http_server(int(ALL_CONFIG_DIC.get("http").get("port")))

    # ticker 刷新target并send
    run_sync_target_thread()


if __name__ == '__main__':
    import sys

    cf = "config.yaml"
    if len(sys.argv) == 2 and sys.argv[1] == "test":
        cf = "test_config.yaml"

    run(cf)
