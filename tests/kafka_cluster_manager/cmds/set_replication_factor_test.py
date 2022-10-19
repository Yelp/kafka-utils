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
from kafka_utils.kafka_cluster_manager.cmds.set_replication_factor \
    import SetReplicationFactorCmd


@pytest.fixture(scope='module')
def mock_zk():
    zk = mock.Mock()
    zk.get_topics.side_effect = (lambda topic_id: {
        topic_id: {
            'partitions': {
                '0': {'isr': ['0', '1']},
                '1': {'isr': ['1', '2']},
            },
        },
    })
    return zk


class TestSetReplicationFactorCmd:

    def test_run_command_add_replica(self, create_cluster_topology, mock_zk):
        assignment = {
            ('T0', 0): ['0', '1'],
            ('T0', 1): ['1', '2'],
        }
        brokers = {
            '0': {'host': 'host2'},
            '1': {'host': 'host2'},
            '2': {'host': 'host3'},
            '3': {'host': 'host4'},
            '4': {'host': 'host5'},
        }
        with mock.patch.object(SetReplicationFactorCmd, 'process_assignment'):
            cmd = SetReplicationFactorCmd()
            cmd.args = mock.Mock(spec=Namespace)
            cmd.args.topic = 'T0'
            cmd.args.replication_factor = 5
            cmd.args.rf_mismatch = False
            cmd.zk = mock_zk
            ct = create_cluster_topology(assignment, brokers)
            cb = PartitionCountBalancer(ct, cmd.args)
            cmd.run_command(ct, cb)

            assert cmd.process_assignment.call_count == 1
            args, kwargs = cmd.process_assignment.call_args_list[0]

            assignment = args[0]
            assert len(assignment) == 2
            assert set(assignment[('T0', 0)]) == {'0', '1', '2', '3', '4'}
            assert set(assignment[('T0', 1)]) == {'0', '1', '2', '3', '4'}

            assert kwargs['allow_rf_change']

    def test_run_command_remove_replica(self, create_cluster_topology, mock_zk):
        assignment = {
            ('T0', 0): ['0', '1', '2', '3', '4'],
            ('T0', 1): ['0', '1', '2', '3', '4'],
        }
        brokers = {
            '0': {'host': 'host2'},
            '1': {'host': 'host2'},
            '2': {'host': 'host3'},
            '3': {'host': 'host4'},
            '4': {'host': 'host5'},
        }
        with mock.patch.object(SetReplicationFactorCmd, 'process_assignment'):
            cmd = SetReplicationFactorCmd()
            cmd.args = mock.Mock(spec=Namespace)
            cmd.args.topic = 'T0'
            cmd.args.replication_factor = 2
            cmd.args.rf_mismatch = False
            cmd.zk = mock_zk

            ct = create_cluster_topology(assignment, brokers)
            cb = PartitionCountBalancer(ct, cmd.args)
            cmd.run_command(ct, cb)

            assert cmd.process_assignment.call_count == 1
            args, kwargs = cmd.process_assignment.call_args_list[0]

            assignment = args[0]
            assert len(assignment) == 2
            assert set(assignment[('T0', 0)]) == {'0', '1'}
            assert set(assignment[('T0', 1)]) == {'1', '2'}

            assert kwargs['allow_rf_change']

    def test_rf_mismatch(self, create_cluster_topology, mock_zk):
        assignment = {
            ('T0', 0): ['0', '1', '2', '3', '4'],
            ('T0', 1): ['0', '1', '2', '3'],
        }
        brokers = {
            '0': {'host': 'host2'},
            '1': {'host': 'host2'},
            '2': {'host': 'host3'},
            '3': {'host': 'host4'},
            '4': {'host': 'host5'},
        }
        with mock.patch.object(SetReplicationFactorCmd, 'process_assignment'):
            cmd = SetReplicationFactorCmd()
            cmd.args = mock.Mock(spec=Namespace)
            cmd.args.topic = 'T0'
            cmd.args.replication_factor = 2
            cmd.args.rf_mismatch = True
            cmd.zk = mock_zk

            ct = create_cluster_topology(assignment, brokers)
            cb = PartitionCountBalancer(ct, cmd.args)
            cmd.run_command(ct, cb)

            assert cmd.process_assignment.call_count == 1
            args, kwargs = cmd.process_assignment.call_args_list[0]

            assignment = args[0]
            assert len(assignment) == 2
            assert set(assignment[('T0', 0)]) == {'0', '1'}
            assert set(assignment[('T0', 1)]) == {'1', '2'}

            assert kwargs['allow_rf_change']
