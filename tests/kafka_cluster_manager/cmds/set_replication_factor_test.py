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
from argparse import Namespace

import mock

from kafka_utils.kafka_cluster_manager.cluster_info.cluster_topology \
    import ClusterTopology
from kafka_utils.kafka_cluster_manager.cmds.set_replication_factor \
    import SetReplicationFactorCmd


class TestSetReplicationFactorCmd(object):

    def test_run_command_add_replica(self):
        assignment = {
            (u'T0', 0): ['0', '1'],
            (u'T0', 1): ['1', '2'],
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
            cmd.args.topic = u'T0'
            cmd.args.replication_factor = 5
            ct = ClusterTopology(assignment, brokers)
            cmd.run_command(ct)

            assert cmd.process_assignment.call_count == 1
            args, kwargs = cmd.process_assignment.call_args_list[0]

            assignment = args[0]
            assert len(assignment) == 2
            assert set(assignment[(u'T0', 0)]) == set(['0', '1', '2', '3', '4'])
            assert set(assignment[(u'T0', 1)]) == set(['0', '1', '2', '3', '4'])

            assert kwargs['allow_rf_change']

    def test_run_command_remove_replica(self):
        assignment = {
            (u'T0', 0): ['0', '1', '2', '3', '4'],
            (u'T0', 1): ['0', '1', '2', '3', '4'],
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
            cmd.args.topic = u'T0'
            cmd.args.replication_factor = 2
            cmd.zk = mock.Mock()
            cmd.zk.get_topics.side_effect = (lambda topic_id: {
                topic_id: {
                    'partitions': {
                        '0': {'isr': ['0', '1']},
                        '1': {'isr': ['1', '2']},
                    },
                },
            })

            ct = ClusterTopology(assignment, brokers)
            cmd.run_command(ct)

            assert cmd.process_assignment.call_count == 1
            args, kwargs = cmd.process_assignment.call_args_list[0]

            assignment = args[0]
            assert len(assignment) == 2
            assert set(assignment[(u'T0', 0)]) == set(['0', '1'])
            assert set(assignment[(u'T0', 1)]) == set(['1', '2'])

            assert kwargs['allow_rf_change']
