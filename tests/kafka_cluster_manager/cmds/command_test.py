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
from collections import OrderedDict

from pytest import fixture

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
def cmd():
    return ClusterManagerCmd()


class TestClusterManagerCmd(object):

    def test_reduced_proposed_plan_no_change(self, cmd, orig_assignment):
        # Provide same assignment
        proposed_assignment = cmd.get_reduced_assignment(
            original_assignment=orig_assignment,
            new_assignment=orig_assignment,
            max_partition_movements=1,
            max_leader_only_changes=1,
        )

        # Verify no proposed plan
        assert proposed_assignment == {}

    def test_extract_actions_unique_topics_limited_actions(self, cmd):
        movements_count = [
            ((u'T0', 0), 1),
            ((u'T0', 1), 1),
            ((u'T1', 0), 1),
            ((u'T2', 0), 1),
        ]
        red_actions = cmd._extract_actions_unique_topics(
            movements_count,
            3,
        )

        assert len(red_actions) == 3
        # Verify that all actions have unique topic
        topics = [action[0] for action in red_actions]
        assert set(topics) == set([u'T0', u'T1', u'T2'])

    def test_extract_actions_partition_movement_no_action(self, cmd):
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
        )

        # All actions have minimum of 2 movements
        # so reduced proposed-plan is empty
        assert red_actions == []

    def test_extract_actions_partition_movements_all(self, cmd):
        movements_count = [
            ((u'T0', 0), 2),
            ((u'T0', 1), 1),
            ((u'T1', 0), 2),
            ((u'T2', 0), 1),
        ]
        red_actions = cmd._extract_actions_unique_topics(
            movements_count,
            10,
        )

        # Total movements as in proposed-plan is 6 (2+1+2+1)
        # max partition-movements allowed is 10, so all 6 partition-movements
        # allowed
        assert len(red_actions) == 4
        assert all(
            t_p in red_actions
            for t_p in ((u'T0', 0), (u'T0', 1), (u'T1', 0), (u'T2', 0))
        )

    def test_extract_actions_no_movements(self, cmd):
        movements_count = [
            ((u'T0', 0), 2),
            ((u'T0', 1), 1),
            ((u'T1', 0), 2),
            ((u'T2', 0), 1),
        ]
        red_actions = cmd._extract_actions_unique_topics(
            movements_count,
            0,
        )

        assert red_actions == []

    def test_extract_actions_unique_topics_some_actions(self, cmd):
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
        )

        assert len(red_actions) == 3
        # Verify T0, T1 and T2 are all in the result
        topics = [action[0] for action in red_actions]
        assert set(topics) == set([u'T0', u'T1', u'T2'])

    def test_reduced_proposed_plan_zero_changes(
        self,
        cmd,
        orig_assignment,
        new_assignment,
    ):
        # Provide less than max_changes parameter
        proposed_assignment = cmd.get_reduced_assignment(
            orig_assignment,
            new_assignment,
            max_partition_movements=0,
            max_leader_only_changes=0,
        )

        # Verify no proposed plan
        assert proposed_assignment == {}

    def test_reduced_proposed_plan_zero_negative_changes(
        self,
        cmd,
        orig_assignment,
        new_assignment,
    ):
        proposed_assignment = cmd.get_reduced_assignment(
            orig_assignment,
            new_assignment,
            max_partition_movements=-1,
            max_leader_only_changes=-1,
        )

        # Verify no proposed plan
        assert proposed_assignment == {}

    def test_reduced_proposed_plan_empty_new_assignment(self, cmd, orig_assignment):
        # Provide empty assignment
        proposed_assignment = cmd.get_reduced_assignment(
            orig_assignment,
            new_assignment={},
            max_partition_movements=1,
            max_leader_only_changes=1,
        )

        # Verify no proposed plan
        assert proposed_assignment == {}

    def test_reduced_proposed_plan_empty_original_assignment(self, cmd, new_assignment):
        proposed_assignment = cmd.get_reduced_assignment(
            original_assignment={},
            new_assignment=new_assignment,
            max_partition_movements=1,
            max_leader_only_changes=1,
        )

        # Verify no proposed plan
        assert proposed_assignment == {}

    def test_reduced_proposed_plan_only_partitions(
        self,
        cmd,
        orig_assignment,
        new_assignment,
    ):
        result = cmd.get_reduced_assignment(
            orig_assignment,
            new_assignment,
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
        new_assignment,
    ):
        result = cmd.get_reduced_assignment(
            orig_assignment,
            new_assignment,
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
        new_assignment,
    ):
        result = cmd.get_reduced_assignment(
            orig_assignment,
            new_assignment,
            max_partition_movements=2,
            max_leader_only_changes=2,
        )

        assert len(result) == 4
        assert (u'T2', 0) in result
        # T1 no changes for 0
        assert (u'T1', 0) not in result and (u'T1', 1) in result
        assert (u'T0', 0) in result and (u'T0', 1) in result
