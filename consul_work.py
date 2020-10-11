import consul.aio
import asyncio
import time
import logging
from consul.base import Timeout
from consistent_hash_ring import ConsistentHashRing
from metrics import *


class Consul(object):
    def __init__(self, host, port):
        '''初始化，连接consul服务器'''
        self.consul = consul.Consul(host, port)
        # self.aio_consul = consul.aio.Consul(host=host, port=port, loop=loop)

    def register_service(self, name, host, port, tags=None):
        tags = tags or []
        # 注册服务
        # id = "{}_{}".format(host, port)
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

    def get_service(self, name):
        services = self.consul.agent.services()
        service = services.get(name)
        if not service:
            return None, None
        addr = "{0}:{1}".format(service['Address'], service['Port'])
        return service, addr

    def block_get_health(self, service_name, service_hash_map, dq):
        index = None
        while True:
            try:
                index, d = self.consul.health.service(service_name, passing=True, index=index)
                if d:
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
                        dq.appendleft(str(service_name))
                        # dq.put(str(service_name))
                        M_SERVICE_CHANGES.labels(service_name=service_name, old_nodes=len(old_nodes),
                                                 new_nodes=len(new_nodes)).set(len(new_nodes))
            except Exception as e:
                logging.error("[watch_error,service:{},error:{}]".format(service_name, e))
                time.sleep(5)
                continue


def start_thread_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def watch_service(service_name, async_consul, service_hash_map, dq):
    # always better to pass ``loop`` explicitly, but this
    # is not mandatory, you can relay on global event loop
    # port = 8500
    # c = consul.aio.Consul(host=host, port=port, loop=loop)
    index = None
    data = None
    # set value, same as default api but with ``await``
    while True:
        try:

            index, d = await async_consul.health.service(service_name, passing=True, index=index)
            if d:
                data = d
                new_nodes = []
                serivce_name = ""
                for x in data:
                    sn = x.get("Service").get("Service")
                    address = x.get("Service").get("Address")
                    if address:
                        new_nodes.append(address)
                    if sn and not serivce_name:
                        serivce_name = sn

                old_nodes = service_hash_map[serivce_name].nodes

                if set(old_nodes) != set(new_nodes):
                    print("[new_num:{} old_num:{}][new_nodes:{} old_nodes:{}]".format(
                        len(new_nodes),
                        len(old_nodes),
                        ",".join(new_nodes),
                        ",".join(old_nodes),

                    ))
                    new_ring = ConsistentHashRing(100, new_nodes)
                    service_hash_map[serivce_name] = new_ring
                    dq.appendleft(str(service_name))

        except Timeout:
            # gracefully handle request timeout
            continue
        except Exception as e:
            print("[watch_error,service:{},error:{}]".format(service_name, e))
            continue


if __name__ == '__main__':
    c = Consul("localhost", 8500, loop=None)
    print(c.get_health("scrape_prome_test"))
