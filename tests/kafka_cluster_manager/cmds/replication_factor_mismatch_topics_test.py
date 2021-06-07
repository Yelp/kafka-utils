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

from argparse import Namespace

import mock

from kafka_utils.kafka_cluster_manager.cluster_info.partition_count_balancer \
    import PartitionCountBalancer
from kafka_utils.kafka_cluster_manager.cmds.replication_factor_mismatch_topics \
    import ReplicationFactorMismatchTopicsCmd


def init_cmd():
    cmd = ReplicationFactorMismatchTopicsCmd()
    cmd.args = mock.Mock(spec=Namespace)
    cmd._print_mismatch_topics = mock.Mock()

    return cmd


def get_assignment_and_brokers(rf_mismatch=False):
    assignment = {
        (u'T0', 0): ['0', '1', '2'] if rf_mismatch else ['0', '1', '2', '3', '4'],
        (u'T0', 1): ['0', '1', '2', '3', '4'],
        (u'T1', 2): ['0', '1'],
        (u'T1', 3): ['0', '1'],
    }
    brokers = {
        '0': {'host': 'host2'},
        '1': {'host': 'host2'},
        '2': {'host': 'host3'},
        '3': {'host': 'host4'},
        '4': {'host': 'host5'},
    }

    return assignment, brokers


class TestReplicationFactorMismatchTopicsCmd(object):

    def test_no_rf_mismatch(self, create_cluster_topology):
        assignment, brokers = get_assignment_and_brokers(rf_mismatch=False)

        cmd = init_cmd()

        ct = create_cluster_topology(assignment, brokers)
        cb = PartitionCountBalancer(ct, cmd.args)
        cmd.run_command(ct, cb)

        assert cmd._print_mismatch_topics.call_count == 1
        cmd._print_mismatch_topics.assert_called_with([])

    def test_rf_mismatch(self, create_cluster_topology):
        assignment, brokers = get_assignment_and_brokers(rf_mismatch=True)

        cmd = init_cmd()

        ct = create_cluster_topology(assignment, brokers)
        cb = PartitionCountBalancer(ct, cmd.args)
        cmd.run_command(ct, cb)

        assert cmd._print_mismatch_topics.call_count == 1

        rf_mismatch_topics = cmd._print_mismatch_topics.call_args.args[0]
        assert len(rf_mismatch_topics) == 1
        assert rf_mismatch_topics[0].id == u'T0'
