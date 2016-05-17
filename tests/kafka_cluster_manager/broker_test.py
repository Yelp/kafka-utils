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
from mock import Mock
from mock import sentinel
from pytest import fixture

from .helper import create_and_attach_partition
from .helper import create_broker
from kafka_utils.kafka_cluster_manager.cluster_info.broker import Broker
from kafka_utils.kafka_cluster_manager.cluster_info.partition import Partition
from kafka_utils.kafka_cluster_manager.cluster_info.topic import Topic


class TestBroker(object):

    @fixture
    def partition(self):
        mock_topic = Mock(spec=Topic, id='t1')
        return Partition(mock_topic, 0)

    # Initial broker-set empty
    def test_partitions_empty(self):
        broker = Broker('test-broker')

        assert broker.partitions == set()

    def test_partitions(self):
        broker = Broker('test-broker', partitions=set([sentinel.p1, sentinel.p2]))

        assert broker.partitions == set([sentinel.p1, sentinel.p2])

    def test_remove_partition(self, partition):
        p1 = partition
        b1 = Broker('test-broker', partitions=set([p1]))
        p1.add_replica(b1)

        # Remove partition
        b1.remove_partition(p1)

        assert p1 not in b1.partitions
        assert b1 not in p1.replicas

    def test_add_partition(self, create_partition):
        p10 = create_partition('t1', 0)
        p20 = create_partition('t2', 0)
        broker = Broker('test-broker', partitions=set([p10]))

        broker.add_partition(p20)

        assert broker.partitions == set([p10, p20])
        assert p20.replicas == [broker]

    def test_topics(self):
        partitions = set([
            Mock(spec=Partition, topic=sentinel.t1),
            Mock(spec=Partition, topic=sentinel.t1),
            Mock(spec=Partition, topic=sentinel.t2),
        ])
        broker = Broker('test-broker', partitions=partitions)

        assert broker.topics == set([sentinel.t1, sentinel.t2])

    def test_count_partition(self):
        t1 = sentinel.t1
        partitions = set([
            Mock(spec=Partition, topic=t1, replicas=sentinel.r1),
            Mock(spec=Partition, topic=t1, replicas=sentinel.r1),
            Mock(spec=Partition, topic=sentinel.t2, replicas=sentinel.r1),
        ])
        broker = Broker('test-broker', partitions=partitions)

        assert broker.count_partitions(t1) == 2
        assert broker.count_partitions(sentinel.t3) == 0

    def test_move_partition(self, partition):
        victim_partition = partition
        source_broker = Broker('b1', partitions=set([victim_partition]))
        victim_partition.add_replica(source_broker)
        dest_broker = Broker('b2')

        # Move partition
        source_broker.move_partition(victim_partition, dest_broker)

        # Verify partition moved from source to destination broker
        assert victim_partition not in source_broker.partitions
        assert victim_partition in dest_broker.partitions
        # Verify that victim-partition-replicas has replaced
        # source-broker to dest-broker
        assert source_broker not in victim_partition.replicas
        assert dest_broker in victim_partition.replicas

    def test_count_preferred_replica(self, partition):
        p1 = partition
        b1 = Broker('test-broker', partitions=set([p1]))
        p1.add_replica(b1)
        p2 = Mock(spec=Partition, topic=sentinel.t1, replicas=[sentinel.b2])
        b1.add_partition(p2)

        assert b1.count_preferred_replica() == 1

    def test_get_preferred_partition(self):
        t1 = Topic('t1', 1)
        t2 = Topic('t2', 1)
        t3 = Topic('t3', 1)
        # Naming convention: p10 -> topic t1 partition 0
        p10 = create_and_attach_partition(t1, 0)
        p11 = create_and_attach_partition(t1, 1)
        p20 = create_and_attach_partition(t2, 0)
        p21 = create_and_attach_partition(t2, 1)
        p30 = create_and_attach_partition(t3, 0)
        source = create_broker('b1', [p10, p11, p20])
        dest = create_broker('b2', [p21, p30])
        sibling_distance = {t1: -2, t2: 0, t3: 1}

        actual = source.get_preferred_partition(dest, sibling_distance)

        assert actual in (p10, p11)

    def test_get_preferred_partition_no_preferred(self):
        t1 = Topic('t1', replication_factor=2)
        p10 = create_and_attach_partition(t1, 0)
        p11 = create_and_attach_partition(t1, 1)
        source = create_broker('b1', [p10, p11])
        dest = create_broker('b2', [p10, p11])
        sibling_distance = {t1: 0}

        actual = source.get_preferred_partition(dest, sibling_distance)

        assert actual is None

    def test_mark_decommissioned(self):
        broker = Broker('test-broker')
        broker.mark_decommissioned()

        assert broker.decommissioned

    def test_empty(self):
        broker = Broker('test-broker')
        # No partitions for this broker
        assert broker.empty()

    def test_not_empty(self, partition):
        broker = Broker('test-broker', partitions=set([partition]))

        assert not broker.empty()

    def test_mark_inactive(self):
        broker = Broker('test-broker')
        broker.mark_inactive()

        assert broker.inactive
