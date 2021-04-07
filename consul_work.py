import consul
import time
import logging
from consistent_hash_ring import ConsistentHashRing
from metrics import *


class Consul(object):
    def __init__(self, host, port):
        '''初始化，连接consul服务器'''
        self.consul = consul.Consul(host, port)

    def register_service(self, name, host, port, tags=None):
        tags = tags or []
        # 注册服务
        id = "{}_{}_{}".format(name, host, port)
        return self.consul.agent.service.register(
            name,
            id,
            host,
            port,
            tags,
            # 健康检查ip端口，检查时间：5,超时时间：30，注销时间：30s
            # check=consul.Check().tcp(host, port, "5s", "5s", "60s"))
            check=consul.Check().tcp(host, port, "5s", "5s"))

    def get_all_service(self):
        return self.consul.agent.services()

    def get_service(self, name):
        services = self.consul.agent.services()
        service = services.get(name)
        if not service:
            return None, None
        addr = "{0}:{1}".format(service['Address'], service['Port'])
        return service, addr

    def get_service_health_node(self, service_name):
        index, data = self.consul.health.service(service_name, passing=True)
        new_nodes = []
        for x in data:
            address = x.get("Service").get("Address")
            if address:
                new_nodes.append(address)
        return new_nodes

    def watch_service(self, service_name, service_hash_map, sync_q):
        index = None
        while True:
            try:
                last_index = index

                index, d = self.consul.health.service(service_name, passing=True, index=index, wait='10s')
                if last_index == None or last_index == index:
                    # 索引没变说明结果没变化，无需处理
                    # last_index == None 代表第一次处理
                    continue

                msg = "[节点变化，需要收敛][service:{}]".format(service_name)
                logging.warning(msg)
                data = d
                new_nodes = []
                for x in data:
                    address = x.get("Service").get("Address")
                    if address:
                        new_nodes.append(address)
                old_nodes = service_hash_map[service_name].nodes

                if set(old_nodes) != set(new_nodes):
                    logging.info("[new_num:{} old_num:{}][new_nodes:{} old_nodes:{}]".format(
                        len(new_nodes),
                        len(old_nodes),
                        ",".join(new_nodes),
                        ",".join(old_nodes),

                    ))
                    new_ring = ConsistentHashRing(100, new_nodes)
                    service_hash_map[service_name] = new_ring
                    sync_q.put(str(service_name))

            except Exception as e:
                logging.error("[watch_error,service:{},error:{}]".format(service_name, e))
                time.sleep(5)
                continue


if __name__ == '__main__':
    c = Consul("172.20.70.205", 8500)
    # print(c.get_service_health_node("scrape_prome_ecs_inf"))
    print(c.get_all_service())
    # print(c.get_service_health_node("scrape_prome_ecs_inf"))
