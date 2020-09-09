import requests
import argparse

from kafka_utils.kafka_cluster_manager.cluster_info.partition import Partition
from kafka_utils.kafka_cluster_manager.cluster_info.topic import Topic

from kafka_utils.kafka_cluster_manager.cluster_info.partition_measurer import PartitionMeasurer

condition = 'app="kafka-cluster", namespace="team-data", kafka_cluster="events", ' \
            'topic!~"__consumer_offsets|events|^public.*|^__.*" '

bytes_per_sec_by_topic = """max(
  sum(
    rate(kafka_server_BrokerTopicMetrics_BytesOutPerSec_Count{{{0}}}[2m])
    +
    rate(kafka_server_BrokerTopicMetrics_BytesInPerSec_Count{{{0}}}[2m])
  ) by (topic, kubernetes_pod_name)
) by (topic)""".format(condition)

log_bytes_by_partition = """max(
  sum(
    kafka_log_Log_Size_Value{{{0}}}
  ) by (topic, partition, kubernetes_pod_name)
) by (topic, partition)""".format(condition)


def query_prom(_prom_url, _query):
    response = requests.get(_prom_url + '/api/v1/query',
                            params={
                                'query': _query,
                            })
    return response.json()['data']['result']


# TODO: weight or size may be ZERO!
def parse_size_and_weights(_prom_url):
    sizes = {}
    num_pars = {}
    weights = {}
    for e in query_prom(_prom_url, log_bytes_by_partition):
        if len(e['metric']) == 2:
            topic, par = e['metric']['topic'], e['metric']['partition']
            key = (topic, par)
            val = float(e['value'][1])
            sizes[key] = val
            num_pars[topic] = num_pars.get(topic, []) + [val]

    for e in query_prom(_prom_url, bytes_per_sec_by_topic):
        if len(e['metric']) == 1:
            topic = e['metric']['topic']
            val = float(e['value'][1])
            if topic in num_pars:
                num_par = len(num_pars[topic])
                for i in range(num_par):
                    key = (topic, str(i))
                    weights[key] = val / num_par
    print(sizes, weights)
    return sizes, weights


class PromMeasurer(PartitionMeasurer):

    def __init__(self, cluster_config, brokers, assignment, args):
        super(PromMeasurer, self).__init__(
            cluster_config,
            brokers,
            assignment,
            args
        )
        self.sizes, self.weights = parse_size_and_weights(self.args.metric_url)

    def parse_args(self, measurer_args):
        parser = argparse.ArgumentParser(prog='PromMeasurer')
        parser.add_argument(
            '--metric-url',
            type=string,
            required=True,
            help='URL of the metric service.',
        )
        return parser.parse_args(measurer_args, self.args)

    def get_weight(self, partition_name):
        topic, par = partition_name[0], partition_name[1]
        return self.weights.get((topic, par), 1)

    def get_size(self, partition_name):
        topic, par = partition_name[0], partition_name[1]
        return self.sizes.get((topic, par), 1)
