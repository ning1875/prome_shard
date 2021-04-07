import requests
import logging

"""
get_targets.py 是prome_shard发现采集targets pool的方法
使用时需要实现 GetTarget的方法，方法名需要和config.yaml中相同
比如 在config.yaml中 有个job名为scrape_prome_ecs_inf
那么需要在 GetTarget中定义方法
    @classmethod
    def scrape_prome_ecs_inf(cls):
    
这个方法返回值是发现到的target列表，形如
    ```python
    [{
        "labels": {
          "group": "SGT",
          "env": "prod",
          "service": "scrape_prome",
          "region": "ap-southeast-3",
          "scrape_type": "vm",
        },
        "targets": [
          "1.1.1.1:9090"
        ]
    }]
    ```
prome_shard根据返回的targets池做一致性哈希分配给配置中定义好的nodes
"""

PORT_NODE_EXPORTER = "9100"
PORT_KAFKA1_EXPORTER = "9308"

PORT_KAFKA2_EXPORTER = "9309"
PORT_ES_EXPORTER = "9114"
PORT_CLICKHOUSE_EXPORTER = "9116"
PORT_BUSINESS_COMMON = PORT_SHARESTORE_EXPORTER = "10106"
PORT_ZOOKEEPER_EXPORTER = "9141"

G_TW = 5


class GetTarget(object):
    # 可以是服务树地址或cmdb
    tree_url = "http://localhost:9993/stree-index"
    cf = "config.yaml"

    @classmethod
    def get_ecs_inf_common(cls, req_data):
        d = None
        try:
            d = cls.get_ecs_inf_common_real(req_data)
        except Exception as e:
            logging.error("[get_ecs_inf_common_error][req:{}][error:{} ] ".format(str(req_data), e))
        finally:
            return d

    @classmethod
    def get_ecs_inf_common_real(cls, req_data):
        """
        获取ecs 9100 基础监控
        :param req_data:
        :return:
        """

        if req_data.get("resource_type") != "ecs":
            return

        query_uri = "{}/query/resource?get_all=1".format(cls.tree_url)
        res = requests.post(query_uri, json=req_data, timeout=G_TW)
        if res.status_code != 200:
            logging.error("bad status_code:{}  error:{}".format(res.status_code, res.text))
            return
        rep = res.json()
        if not rep:
            logging.error("[get_ecs_inf_common_error][rep_empty]")
            return
        result = rep.get("result")
        if not result:
            logging.error("[get_ecs_inf_common_error][result_empty]")
            return
        targets = result
        logging.info("[get_ecs_inf_common_res][req:{} get:{}]".format(str(req_data), len(targets)))
        new_targets = []
        for i in targets:
            i_hash = i.get("hash")
            if not i_hash:
                continue

            tags = i.get("tags")
            if not tags:
                continue
            private_ip = i.get("private_ip")
            if not private_ip:
                continue
            name = i.get("name")
            if not name:
                continue
            addr = private_ip[0]
            region = i.get("region")

            env = tags.get("env")
            group = tags.get("group")
            project = tags.get("project")
            subgroup = tags.get("subgroup")
            stree_app = tags.get("stree-app")
            stree_project = tags.get("stree-project")

            gpa = "{}.{}.{}".format(
                group,
                stree_project,
                stree_app,
            )
            labels = {
                "name": name,
                "region": region,
                "env": env,
                "group": group,
                "project": project,
                # "subgroup": subgroup,
                "stree_gpa": gpa,
            }

            ins = ["{}:{}".format(addr, PORT_NODE_EXPORTER)]
            if subgroup:
                labels["subgroup"] = subgroup

            dd = {
                "targets": ins,
                "labels": labels
            }
            new_targets.append(dd)
        return new_targets

    @classmethod
    def scrape_prome_ecs_inf(cls):
        req_data = {
            'resource_type': 'ecs',
            'use_index': True,
            'labels': [
                {'key': 'env', 'value': 'prod', 'type': 1},
                {'key': 'group', 'value': 'BDP', 'type': 2},
                {'key': 'subgroup', 'value': 'MRS', 'type': 2},
                {'key': 'subgroup', 'value': 'BigData', 'type': 2},
                {'key': 'subgroup', 'value': 'BigData2', 'type': 2},
            ]
        }
        # TODO mock data
        targets = []
        for x in range(1, 100):
            l = {
                "targets": [
                    "172.20.70.{}:9100".format(x)
                ],
                "labels": {
                    "name": "prometheus-master-01",
                    "account": "aliyun-01",
                    "region": "ap-south-1",
                    "env": "prod",
                    "group": "inf",
                    "project": "monitor",
                    "stree_gpa": "inf.monitor.prometheus"
                }
            }
            targets.append(l)
        return targets
        new_targets = cls.get_ecs_inf_common(req_data)
        return new_targets


if __name__ == "__main__":
    res = GetTarget.scrape_prome_ecs_inf()
