# -*- coding: utf-8 -*-
# Copyright 2021 Yelp Inc.
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
from __future__ import absolute_import

from .command import ClusterManagerCmd


class ReplicationFactorMismatchTopicsCmd(ClusterManagerCmd):

    def __init__(self):
        super(ReplicationFactorMismatchTopicsCmd, self).__init__()

    def build_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            'replication-factor-mismatch-topics',
            description='Get topics with replication factor mismatch among its partitions.',
            help='This command is used to find which topics have partitions with replicas less than the configured '
                 'replication factor of the topic.',
        )
        return subparser

    def _print_mismatch_topics(self, rf_mismatch_topics):
        print("{mismatch_count} topic(s) with replication factor mismatch found".format(mismatch_count=len(rf_mismatch_topics)))
        for topic in rf_mismatch_topics:
            print("\tTopic ID: {topic_id}".format(topic_id=topic.id))
            print("\t\tPartitions: {partitions}".format(partitions=topic.partitions))

    def run_command(self, ct, cluster_balancer):
        rf_mismatch_topics = []

        for topic in ct.topics.values():
            # Kafka considers first partition's replica count as the replication factor of the topic
            # So that, any of the partitions having a different replica count means there is a mismatch
            # https://github.com/apache/kafka/blob/77a89fcf8d7fa01831683327727f11f4bed97319/core/src/main/scala/kafka/admin/TopicCommand.scala#L286
            partitions_replication_count = {len(partition.replicas) for partition in topic.partitions}

            if len(partitions_replication_count) > 1:
                rf_mismatch_topics.append(topic)

        self._print_mismatch_topics(rf_mismatch_topics)
