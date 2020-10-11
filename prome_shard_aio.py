import json
import logging
import yaml
import asyncio
import time

from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
from collections import deque
from consul_work import start_thread_loop, watch_service, Consul
from get_targets import GetTarget
from threading import Thread
from consistent_hash_ring import ConsistentHashRing

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
SYNC_DQ = deque()
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
    func = getattr(GetTarget, service_name)
    if not func:
        logging.info("[reflect_error][func:{}not_found in GetTarget]".format(
            service_name,
        ))
        return

    hash_ring = SERVICE_HASH_MAP.get(service_name)
    if not hash_ring:
        logging.info("[hash_ring_error][func:{} not_found in SERVICE_HASH_MAP]".format(
            service_name,
        ))
        return

    targets = func()
    if not targets:
        logging.info("[run_func_error_GetTarget.{}]".format(
            service_name,
        ))
        return

    if len(targets) == 0:
        logging.info("[run_func_Get_empty_Target.{}]".format(
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
        target_node = hash_ring.get_node(str(index))

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
    #
    # pool = ThreadPool(20)
    # pool.map(ansible_send_work, all_args)
    #
    # pool.close()
    # pool.join()

    for i in all_args:
        ansible_send_work(i)


def ansible_send_work(args):
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


def consumer():
    while True:
        if SYNC_DQ:
            msg = SYNC_DQ.pop()
            if msg:
                shard_job(msg)


def run_sync_target_multi():
    while True:
        services = SERVICE_HASH_MAP.keys()

        with Pool(processes=len(services)) as pool:
            pool.map(shard_job, services)

        # pool = ThreadPool(20)
        # pool.map(ansible_send_work, services)
        #
        # pool.close()
        # pool.join()

        time.sleep(int(ALL_CONFIG_DIC.get("job_setting").get("ticker_interval", 600)))


def run_sync_target():
    while True:
        services = SERVICE_HASH_MAP.keys()

        for i in services:
            shard_job(i)

        time.sleep(int(ALL_CONFIG_DIC.get("job_setting").get("ticker_interval", 600)))


def run_sync_target_thread():
    while True:
        services = SERVICE_HASH_MAP.keys()

        logging.info("[main_loo_run....][service_num:{}][detail:]".format(
            len(services),
            ",".join(services)
        ))
        pool = ThreadPool(len(services))
        pool.map(shard_job, services)

        pool.close()
        pool.join()

        time.sleep(int(ALL_CONFIG_DIC.get("job_setting").get("ticker_interval", 600)))


def run():
    yaml_path = "config.yaml"

    config = load_base_config(yaml_path)
    global ALL_CONFIG_DIC
    ALL_CONFIG_DIC = config
    shard_service_d = config.get("shard_service")

    logging.info("[start_for_service][service_num:{}][detail:{}]".format(
        len(shard_service_d.keys()),
        ",".join(shard_service_d.keys()),

    ))

    # ioloop
    loop = asyncio.get_event_loop()

    # consul
    consul_addr = config.get("consul").get("host")
    consul_port = int(config.get("consul").get("port"))
    consul_obj = Consul(consul_addr, consul_port, loop)

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

    # 开启watch变化结果队列消费线程
    consumer_thread = Thread(target=consumer)
    consumer_thread.setDaemon(True)
    consumer_thread.start()

    # 开启consul watch
    loop_thread = Thread(target=start_thread_loop, args=(loop,))
    loop_thread.setDaemon(True)
    loop_thread.start()
    for service_name in shard_service_d.keys():
        asyncio.run_coroutine_threadsafe(watch_service(service_name, consul_obj.aio_consul, SERVICE_HASH_MAP, SYNC_DQ),
                                         loop)

    # ticker 刷新target并send

    run_sync_target_thread()
    # run_sync_target_multi()
    # run_sync_target()


if __name__ == '__main__':
    run()
