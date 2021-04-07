import json
import shutil
from ansible.module_utils.common.collections import ImmutableDict
from ansible.parsing.dataloader import DataLoader
from ansible.vars.manager import VariableManager
from ansible.inventory.manager import InventoryManager
from ansible.playbook.play import Play
from ansible.executor.task_queue_manager import TaskQueueManager
from ansible.executor.playbook_executor import PlaybookExecutor
from ansible.plugins.callback import CallbackBase
from ansible import context

import ansible.constants as C
import logging


# logging.basicConfig(
#     format='%(asctime)s %(levelname)s %(filename)s [func:%(funcName)s] [line:%(lineno)d]:%(message)s',
#     datefmt="%Y-%m-%d %H:%M:%S",
#     level="INFO"
# )

class ResultCallback(CallbackBase):
    """
    重写callbackBase类的部分方法
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.host_ok = {}
        self.host_unreachable = {}
        self.host_failed = {}
        self.task_ok = {}

    def v2_runner_on_unreachable(self, result):
        self.host_unreachable[result._host.get_name()] = result

    def v2_runner_on_ok(self, result, **kwargs):
        self.host_ok[result._host.get_name()] = result

    def v2_runner_on_failed(self, result, **kwargs):
        self.host_failed[result._host.get_name()] = result


class MyAnsiable2():
    def __init__(self,
                 connection='smart',  # 连接方式 local 本地方式，smart ssh方式
                 remote_user=None,  # 远程用户
                 ack_pass=None,  # 提示输入密码
                 sudo=None, sudo_user=None, ask_sudo_pass=None,
                 module_path=None,  # 模块路径，可以指定一个自定义模块的路径
                 become=None,  # 是否提权
                 become_method=None,  # 提权方式 默认 sudo 可以是 su
                 become_user=None,  # 提权后，要成为的用户，并非登录用户
                 check=False, diff=False,
                 listhosts=None, listtasks=None, listtags=None,
                 verbosity=3,
                 syntax=None,
                 start_at_task=None,
                 inventory=None):

        # 函数文档注释
        """
        初始化函数，定义的默认的选项值，
        在初始化的时候可以传参，以便覆盖默认选项的值
        """
        context.CLIARGS = ImmutableDict(
            connection=connection,
            remote_user=remote_user,
            ack_pass=ack_pass,
            sudo=sudo,
            sudo_user=sudo_user,
            ask_sudo_pass=ask_sudo_pass,
            module_path=module_path,
            become=become,
            become_method=become_method,
            become_user=become_user,
            verbosity=verbosity,
            listhosts=listhosts,
            listtasks=listtasks,
            listtags=listtags,
            syntax=syntax,
            start_at_task=start_at_task,
        )

        # 三元表达式，假如没有传递 inventory, 就使用 "localhost,"
        # 确定 inventory 文件
        self.inventory = inventory if inventory else "localhost,"

        # 实例化数据解析器
        self.loader = DataLoader()

        # 实例化 资产配置对象
        self.inv_obj = InventoryManager(loader=self.loader, sources=self.inventory)

        # 设置密码，可以为空字典，但必须有此参数
        self.passwords = {}

        # 实例化回调插件对象
        self.results_callback = ResultCallback()

        # 变量管理器
        self.variable_manager = VariableManager(self.loader, self.inv_obj)

    def run(self, hosts='localhost', gether_facts="no", module="ping", args=''):
        play_source = dict(
            name="Ad-hoc",
            hosts=hosts,
            gather_facts=gether_facts,
            tasks=[
                # 这里每个 task 就是这个列表中的一个元素，格式是嵌套的字典
                # 也可以作为参数传递过来，这里就简单化了。
                {"action": {"module": module, "args": args}},
            ])

        play = Play().load(play_source, variable_manager=self.variable_manager, loader=self.loader)

        tqm = None
        try:
            tqm = TaskQueueManager(
                inventory=self.inv_obj,
                variable_manager=self.variable_manager,
                loader=self.loader,
                passwords=self.passwords,
                stdout_callback=self.results_callback)

            result = tqm.run(play)
        finally:
            if tqm is not None:
                tqm.cleanup()
            shutil.rmtree(C.DEFAULT_LOCAL_TMP, True)

    def playbookrun(self, host_list, playbook_paths, extra_var):

        self.variable_manager._extra_vars = extra_var
        sources = ','.join(host_list)
        if len(host_list) == 1:
            sources += ','
        self.inventory = InventoryManager(loader=self.loader, sources=sources)
        playbook = PlaybookExecutor(playbooks=playbook_paths,  # 注意这里是一个列表
                                    # inventory=self.inv_obj,
                                    inventory=self.inventory,
                                    variable_manager=self.variable_manager,
                                    loader=self.loader,
                                    passwords=self.passwords
                                    )

        # 使用回调函数
        playbook._tqm._stdout_callback = self.results_callback

        result = playbook.run()
        return result

    def get_result(self):
        results_raw = {'success': {}, 'failed': {}, "unreachable": {}}

        for host, result in self.results_callback.host_ok.items():
            results_raw['success'][host] = json.dumps(result._result)

        for host, result in self.results_callback.host_failed.items():
            results_raw['failed'][host] = result._result['msg']

        for host, result in self.results_callback.host_unreachable.items():
            results_raw['unreachable'][host] = result._result['msg']

        return results_raw


def run_play_with_res(ips, yaml_path, extra_vars):
    t = MyAnsiable2()
    msg = "[run_play][ips:{}][yaml_path:{}][extra_vars:{}]".format(
        ips,
        yaml_path,
        extra_vars,
    )
    # logging.info(msg)
    res = t.playbookrun(ips, [yaml_path], extra_vars)
    # logging.info("[res:{}]".format(res))
    return t.get_result()


def run_play(ips, yaml_path, extra_vars):
    t = MyAnsiable2()
    # print("run_play", ips, yaml_path, extra_vars)

    t.playbookrun(ips, [yaml_path], extra_vars)
    return None
    return t.get_result()


if __name__ == "__main__":
    host_list = ["172.20.70.215"]
    res = run_play_with_res(host_list, "./copy_file_and_reload_prome.yaml", extra_vars={
        "src_sd_file_name": "requirements.txt",
        "dest_sd_file_name": "requirements.txt",
        "service_port": 9090,

    })
    print(res)
