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
from __future__ import absolute_import

from collections import OrderedDict

import mock
from pytest import fixture
from pytest import raises

from kafka_utils.kafka_cluster_manager.cluster_info.cluster_balancer \
    import ClusterBalancer
from kafka_utils.kafka_cluster_manager.cluster_info.cluster_topology \
    import ClusterTopology
from kafka_utils.kafka_cluster_manager.cluster_info.partition_measurer \
    import UniformPartitionMeasurer
from kafka_utils.kafka_cluster_manager.cmds.command import ClusterManagerCmd


@fixture
def orig_assignment():
    return OrderedDict([
        ((u'T1', 1), [2, 1]),
        ((u'T0', 0), [0, 1]),
        ((u'T0', 1), [1, 2]),
        ((u'T1', 0), [0, 1]),
        ((u'T2', 0), [3, 1]),
    ])


# Replica changes for (T0, 0) and (T1, 1)
# Leader only changes (T0, 1) and (T2, 0)
@fixture
def new_assignment():
    return OrderedDict([
        ((u'T0', 0), [2, 0]),
        ((u'T1', 1), [2, 3]),
        ((u'T0', 1), [2, 1]),
        ((u'T1', 0), [0, 1]),
        ((u'T2', 0), [1, 3]),
    ])


@fixture
def cluster_topology(new_assignment):
    """ This topology contains the new_assignment
    """
    brokers = {0: None, 1: None, 2: None, 3: None}
    pm = UniformPartitionMeasurer({}, brokers, new_assignment, {})
    return ClusterTopology(new_assignment, brokers, pm)


@fixture
def empty_cluster_topology():
    """ This topology contains an empty assignment
    """
    brokers = {0: None, 1: None, 2: None, 3: None}
    pm = UniformPartitionMeasurer({}, brokers, {}, {})
    return ClusterTopology({}, brokers, pm)


@fixture
def orig_cluster_topology(orig_assignment):
    """ This topology contains the original assignment
    """
    brokers = {0: None, 1: None, 2: None, 3: None}
    pm = UniformPartitionMeasurer({}, brokers, orig_assignment, {})
    return ClusterTopology(orig_assignment, brokers, pm)


@fixture
def cmd():
    return ClusterManagerCmd()


class TestClusterManagerCmd(object):

    def test_reduced_proposed_plan_no_change(self, cmd, orig_assignment, orig_cluster_topology):
        # Provide same assignment
        proposed_assignment = cmd.get_reduced_assignment(
            original_assignment=orig_assignment,
            cluster_topology=orig_cluster_topology,
            max_partition_movements=1,
            max_leader_only_changes=1,
        )

        # Verify no proposed plan
        assert proposed_assignment == {}

    def test_extract_actions_unique_topics_limited_actions(self, cmd, cluster_topology):
        movements_count = [
            ((u'T0', 0), 1),
            ((u'T0', 1), 1),
            ((u'T1', 0), 1),
            ((u'T2', 0), 1),
        ]
        red_actions = cmd._extract_actions_unique_topics(
            movements_count,
            3,
            cluster_topology,
            float('inf'),
        )

        assert len(red_actions) == 3
        # Verify that all actions have unique topic
        topics = [action[0] for action in red_actions]
        assert set(topics) == set([u'T0', u'T1', u'T2'])

    def test_extract_actions_unique_topics_limited_actions_size(self, cmd, cluster_topology):
        movements_count = [
            ((u'T0', 0), 1),
            ((u'T0', 1), 1),
            ((u'T1', 0), 1),
            ((u'T2', 0), 1),
        ]
        red_actions = cmd._extract_actions_unique_topics(
            movements_count,
            3,
            cluster_topology,
            1,
        )

        assert len(red_actions) == 1

    def test_extract_actions_partition_movement_no_action(self, cmd, cluster_topology):
        # In case max-allowed partition-movements is less than replication-factor
        # there is a possibility it will never converge
        movements_count = [
            ((u'T0', 0), 2),
            ((u'T0', 1), 2),
            ((u'T1', 0), 2),
            ((u'T2', 0), 2),
        ]
        red_actions = cmd._extract_actions_unique_topics(
            movements_count,
            1,
            cluster_topology,
            float('inf'),
        )

        # All actions have minimum of 2 movements
        # so reduced proposed-plan is empty
        assert red_actions == []

    def test_extract_actions_partition_movements_all(self, cmd, cluster_topology):
        movements_count = [
            ((u'T0', 0), 2),
            ((u'T0', 1), 1),
            ((u'T1', 0), 2),
            ((u'T2', 0), 1),
        ]
        red_actions = cmd._extract_actions_unique_topics(
            movements_count,
            10,
            cluster_topology,
            float('inf'),
        )

        # Total movements as in proposed-plan is 6 (2+1+2+1)
        # max partition-movements allowed is 10, so all 6 partition-movements
        # allowed
        assert len(red_actions) == 4
        assert all(
            t_p in red_actions
            for t_p in ((u'T0', 0), (u'T0', 1), (u'T1', 0), (u'T2', 0))
        )

    def test_extract_actions_no_movements(self, cmd, cluster_topology):
        movements_count = [
            ((u'T0', 0), 2),
            ((u'T0', 1), 1),
            ((u'T1', 0), 2),
            ((u'T2', 0), 1),
        ]
        red_actions = cmd._extract_actions_unique_topics(
            movements_count,
            0,
            cluster_topology,
            float('inf'),
        )

        assert red_actions == []

    def test_extract_actions_unique_topics_some_actions(self, cmd, cluster_topology):
        # Complex case
        # Total proposed-movements: 2*4 + 1 = 9
        # Max allowed movements: 5
        # Total actions in proposed_assignment: 5
        # Expected: Final assignment should have all 3 actions
        # all 3 unique topics
        movements_count = [
            ((u'T0', 0), 2),
            ((u'T1', 1), 2),
            ((u'T0', 1), 2),
            ((u'T1', 0), 2),
            ((u'T2', 0), 1),
        ]
        red_actions = cmd._extract_actions_unique_topics(
            movements_count,
            5,
            cluster_topology,
            float('inf'),
        )

        assert len(red_actions) == 3
        # Verify T0, T1 and T2 are all in the result
        topics = [action[0] for action in red_actions]
        assert set(topics) == set([u'T0', u'T1', u'T2'])

    def test_reduced_proposed_plan_zero_changes(
        self,
        cmd,
        orig_assignment,
        cluster_topology,
    ):
        # Provide less than max_changes parameter
        proposed_assignment = cmd.get_reduced_assignment(
            orig_assignment,
            cluster_topology,
            max_partition_movements=0,
            max_leader_only_changes=0,
        )

        # Verify no proposed plan
        assert proposed_assignment == {}

    def test_reduced_proposed_plan_zero_negative_changes(
        self,
        cmd,
        orig_assignment,
        cluster_topology,
    ):
        proposed_assignment = cmd.get_reduced_assignment(
            orig_assignment,
            cluster_topology,
            max_partition_movements=-1,
            max_leader_only_changes=-1,
            max_movement_size=-1,
        )

        # Verify no proposed plan
        assert proposed_assignment == {}

    def test_reduced_proposed_plan_empty_new_assignment(self, cmd, orig_assignment, empty_cluster_topology):
        # Provide empty assignment
        proposed_assignment = cmd.get_reduced_assignment(
            orig_assignment,
            empty_cluster_topology,
            max_partition_movements=1,
            max_leader_only_changes=1,
        )

        # Verify no proposed plan
        assert proposed_assignment == {}

    def test_reduced_proposed_plan_empty_original_assignment(self, cmd, cluster_topology):
        proposed_assignment = cmd.get_reduced_assignment(
            original_assignment={},
            cluster_topology=cluster_topology,
            max_partition_movements=1,
            max_leader_only_changes=1,
        )

        # Verify no proposed plan
        assert proposed_assignment == {}

    def test_reduced_proposed_plan_only_partitions(
        self,
        cmd,
        orig_assignment,
        cluster_topology,
    ):
        result = cmd.get_reduced_assignment(
            orig_assignment,
            cluster_topology,
            max_partition_movements=2,
            max_leader_only_changes=0,
        )

        assert len(result) == 2
        # T2 not in result because leader only change
        assert (u'T2', 0) not in result
        # T1 no changes for 0
        assert (u'T1', 0) not in result and (u'T1', 1) in result
        # T0 leader only changes for 1
        assert (u'T0', 0) in result and (u'T0', 1) not in result

    def test_reduced_proposed_plan_only_leaders(
        self,
        cmd,
        orig_assignment,
        cluster_topology,
    ):
        result = cmd.get_reduced_assignment(
            orig_assignment,
            cluster_topology,
            max_partition_movements=0,
            max_leader_only_changes=2,
        )

        assert len(result) == 2
        # T2 leader only change for 0
        assert (u'T2', 0) in result
        # T1 no leader only changes
        assert (u'T1', 0) not in result and (u'T1', 1) not in result
        # T0 leader only changes for 1
        assert (u'T0', 0) not in result and (u'T0', 1) in result

    def test_reduced_proposed_plan(
        self,
        cmd,
        orig_assignment,
        cluster_topology,
    ):
        result = cmd.get_reduced_assignment(
            orig_assignment,
            cluster_topology,
            max_partition_movements=2,
            max_leader_only_changes=2,
        )

        assert len(result) == 4
        assert (u'T2', 0) in result
        # T1 no changes for 0
        assert (u'T1', 0) not in result and (u'T1', 1) in result
        assert (u'T0', 0) in result and (u'T0', 1) in result

    def test_reduced_proposed_plan_max_movement_size(
        self,
        cmd,
        orig_assignment,
        cluster_topology,
    ):
        result = cmd.get_reduced_assignment(
            orig_assignment,
            cluster_topology,
            max_partition_movements=2,
            max_leader_only_changes=2,
            max_movement_size=1,
        )

        # 2 leader changes + only 1 partition movement = 3
        assert len(result) == 3

    @mock.patch('kafka_utils.kafka_cluster_manager.cmds.command.ZK')
    def test_runs_command_with_preconditions(self, mock_zk, cmd):
        cluster_config = mock.MagicMock()
        args = mock.MagicMock()
        mock_zk.return_value.__enter__.return_value = mock.MagicMock(
            get_brokers=lambda: {
                1: {'host': 'host1'},
                2: {'host': 'host2'},
                3: {'host': 'host3'},
            },
            get_assignment=lambda: {},
            get_cluster_assignment=lambda: new_assignment(),
            get_pending_plan=lambda: None,
        )
        rg_parser = mock.MagicMock()
        partition_measurer = UniformPartitionMeasurer
        cluster_balancer = mock.MagicMock(spec=ClusterBalancer)
        cmd.run_command = mock.MagicMock()

        cmd.run(
            cluster_config,
            rg_parser,
            partition_measurer,
            cluster_balancer,
            args,
        )

        assert cmd.run_command.call_count == 1

    @mock.patch('kafka_utils.kafka_cluster_manager.cmds.command.ZK')
    def test_empty_cluster(self, mock_zk, cmd):
        cluster_config = mock.MagicMock()
        args = mock.MagicMock()
        mock_zk.return_value.__enter__.return_value = mock.MagicMock(
            get_brokers=lambda: {
                1: {'host': 'host1'},
                2: {'host': 'host2'},
                3: {'host': 'host3'},
            },
            get_assignment=lambda: {},
            get_pending_plan=lambda: None,
        )
        rg_parser = mock.MagicMock()
        partition_measurer = UniformPartitionMeasurer
        cluster_balancer = mock.MagicMock(spec=ClusterBalancer)
        cmd.run_command = mock.MagicMock()

        cmd.run(
            cluster_config,
            rg_parser,
            partition_measurer,
            cluster_balancer,
            args,
        )

        assert cmd.run_command.call_count == 0

    @mock.patch('kafka_utils.kafka_cluster_manager.cmds.command.ZK')
    def test_exit_on_pending_assignment(self, mock_zk, cmd):
        cluster_config = mock.MagicMock()
        args = mock.MagicMock()
        mock_zk.return_value.__enter__.return_value = mock.MagicMock(
            get_brokers=lambda: {
                1: {'host': 'host1'},
                2: {'host': 'host2'},
                3: {'host': 'host3'},
            },
            get_assignment=lambda: {},
            get_cluster_assignment=lambda: new_assignment(),
            get_pending_plan=lambda: {'partitions': []},
        )
        rg_parser = mock.MagicMock()
        partition_measurer = UniformPartitionMeasurer
        cluster_balancer = mock.MagicMock(spec=ClusterBalancer)
        with raises(SystemExit):
            cmd.run(
                cluster_config,
                rg_parser,
                partition_measurer,
                cluster_balancer,
                args,
            )
