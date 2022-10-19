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
from argparse import Namespace
from unittest import mock

import pytest

from kafka_utils.kafka_cluster_manager.cluster_info.partition_count_balancer \
    import PartitionCountBalancer
from kafka_utils.kafka_cluster_manager.cmds.replace \
    import ReplaceBrokerCmd


@pytest.fixture()
def assignment():
    return {
        ('T0', 0): [0, 1],
        ('T0', 1): [1, 2],
    }


@pytest.fixture()
def brokers():
    return {
        0: {'host': 'host2'},
        1: {'host': 'host2'},
        2: {'host': 'host3'},
        3: {'host': 'host4'},
        4: {'host': 'host5'},
    }


class TestReplaceBrokerCmd:

    def mock_args(self):
        args = mock.Mock(spec=Namespace)
        args.max_leader_changes = 10
        args.max_partition_movements = 10
        args.rf_change = False
        args.rf_mismatch = False
        args.topic_partition_filter = None
        args.source_broker = 0
        args.dest_broker = 3
        return args

    def test_filter_list(self, create_cluster_topology, assignment, brokers):
        with mock.patch.object(ReplaceBrokerCmd, 'process_assignment'), mock.patch.object(ReplaceBrokerCmd, 'get_topic_filter') as mock_filter:
            cmd = ReplaceBrokerCmd()
            cmd.args = self.mock_args()
            cmd.args.topic_partition_filter = 'foo'
            mock_filter.return_value = {('T0', 0)}
            ct = create_cluster_topology(assignment, brokers)
            cb = PartitionCountBalancer(ct, cmd.args)
            cmd.run_command(ct, cb)

            assert cmd.process_assignment.call_count == 1
            args, _ = cmd.process_assignment.call_args_list[0]
            assignment = args[0]
            assert len(assignment) == 1
            assert set(assignment[('T0', 0)]) == {3, 1}

    def test_remove(self, create_cluster_topology, assignment, brokers):
        with mock.patch.object(ReplaceBrokerCmd, 'process_assignment'), mock.patch.object(ReplaceBrokerCmd, 'get_topic_filter') as mock_filter:
            cmd = ReplaceBrokerCmd()
            cmd.args = self.mock_args()
            cmd.args.dest_broker = None
            cmd.args.topic_partition_filter = 'foo'
            cmd.args.rf_change = True
            cmd.args.rf_mismatch = True
            mock_filter.return_value = {('T0', 0)}
            ct = create_cluster_topology(assignment, brokers)
            cb = PartitionCountBalancer(ct, cmd.args)
            cmd.run_command(ct, cb)

            assert cmd.process_assignment.call_count == 1
            args, _ = cmd.process_assignment.call_args_list[0]
            assignment = args[0]
            assert len(assignment) == 1
            assert set(assignment[('T0', 0)]) == {1}

    def test_rf_change(self, create_cluster_topology, assignment, brokers):
        with mock.patch.object(ReplaceBrokerCmd, 'process_assignment'), mock.patch.object(ReplaceBrokerCmd, 'get_topic_filter'):
            cmd = ReplaceBrokerCmd()
            cmd.args = self.mock_args()
            # Remove broker 1
            cmd.args.source_broker = 1
            cmd.args.dest_broker = None
            ct = create_cluster_topology(assignment, brokers)
            cb = PartitionCountBalancer(ct, cmd.args)
            # No rf changes allowed -- should fail
            with pytest.raises(SystemExit):
                cmd.run_command(ct, cb)
            assert cmd.process_assignment.call_count == 0
            # Should still fail even if we set mismatch
            # Need to recreate cluster_topology since it gets modified by previous
            ct = create_cluster_topology(assignment, brokers)
            cmd.args.rf_mismatch = True
            with pytest.raises(SystemExit):
                cmd.run_command(ct, cb)
            assert cmd.process_assignment.call_count == 0
            # Allow rf changes for entire topic, T0
            ct = create_cluster_topology(assignment, brokers)
            cmd.args.rf_change = True
            cmd.run_command(ct, cb)
            assert cmd.process_assignment.call_count == 1

    def test_rf_mismatch(self, create_cluster_topology, assignment, brokers):
        with mock.patch.object(ReplaceBrokerCmd, 'process_assignment'), mock.patch.object(ReplaceBrokerCmd, 'get_topic_filter'):
            cmd = ReplaceBrokerCmd()
            cmd.args = self.mock_args()
            # Remove broker 0
            cmd.args.source_broker = 0
            cmd.args.dest_broker = None
            ct = create_cluster_topology(assignment, brokers)
            cb = PartitionCountBalancer(ct, cmd.args)
            # No rf mismatch allowed -- should fail
            with pytest.raises(SystemExit):
                cmd.run_command(ct, cb)
            assert cmd.process_assignment.call_count == 0
            # Should still fail even if we set rf_change
            cmd.args.rf_change = True
            ct = create_cluster_topology(assignment, brokers)
            with pytest.raises(SystemExit):
                cmd.run_command(ct, cb)
            assert cmd.process_assignment.call_count == 0
            # Allow rf mismatch for T0
            ct = create_cluster_topology(assignment, brokers)
            cmd.args.rf_mismatch = True
            cmd.run_command(ct, cb)
            assert cmd.process_assignment.call_count == 1
