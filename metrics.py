from prometheus_client import start_http_server
from prometheus_client import Gauge

M_SHARD_SERVICE_DES = Gauge('shard_service_des', 'Description of shard service', ['service_name', 'service_port', ])
M_ANSIBLE_TIME = Gauge('ansible_send_work_processing_seconds', 'Time spent ansible work ',
                       ['ip', 'dest_sd_file_name', 'src_sd_file_name', 'service_port', 'yaml_path'])
M_SERVICE_CHANGES = Gauge('consul_service_change_res', 'service change by consul watch',
                          ['service_name', 'old_nodes', 'new_nodes'])

M_GET_TARGET_RESULT_NUM = Gauge('get_targets_result_num', 'get_targets_result', ['service_name'])
M_GET_TARGET_RESULT_TIME = Gauge('get_targets_result_time', 'get_targets_result', ['service_name'])
