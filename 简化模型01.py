import time
import os
import logging
from multiprocessing import Process, Queue
import multiprocessing_logging

multiprocessing_logging.install_mp_handler()
logging.basicConfig(
    format='%(asctime)s %(levelname)s %(filename)s [func:%(funcName)s] [line:%(lineno)d]:%(message)s',
    datefmt="%Y-%m-%d %H:%M:%S",
    level="INFO"
)
logger = logging.getLogger()


def consumer(sync_dq):
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


def watch_service(sync_dq):
    """
    1.watch concul的函数
    2.节点变化后塞入队列中
    :param sync_dq:  用来同步节点变化的队列
    :return:
    """
    msg = "[start service watch_service..[pid:{}]".format(os.getpid())
    logger.info(msg)
    n = 0

    while True:

        if n % 10 == 0:
            msg = "节点有变，发送任务: n={}".format(n)
            logger.info(msg)
            sync_dq.put(str(n))
        time.sleep(1)
        n += 1


def cron_sync_targets():
    """
    在golang中的ticker模式
    1.定时从服务树获取targets
    2.发往存活的采集器
    3.reload 采集器
    :return:
    """
    msg = "[start service cron_sync_targets..[pid:{}]".format(os.getpid())
    logger.info(msg)
    while True:
        msg = "[cron_sync_targets][run_loop]"
        logger.info(msg)
        time.sleep(5)


if __name__ == "__main__":
    q = Queue()

    p1 = Process(target=cron_sync_targets, )
    p1.start()

    p2 = Process(target=watch_service, args=(q,))
    p2.start()

    p3 = Process(target=consumer, args=(q,))
    p3.start()

    p1.join()
    p2.join()
    p3.join()
