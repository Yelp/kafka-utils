# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from kafka import KafkaClient


def _prepare_host_list(broker_list):
    """Returns string with broker_list hosts, compatible format with KafkaClient ctor.
    String format:
        * string: '{host}:{port}, ...'
    """
    return ','.join(
        [
            '{host}:{port}'.format(host=broker_info['host'], port=broker_info['port'])
            for broker_info in broker_list.values()
        ]
    )


def get_topic_partition_metadata(broker_list):
    """Returns topic-partition metadata from Kafka broker."""
    hosts = _prepare_host_list(broker_list)
    kafka_client = KafkaClient(hosts, timeout=10)
    return kafka_client.topic_partitions
