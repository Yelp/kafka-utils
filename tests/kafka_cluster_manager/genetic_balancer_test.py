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
from __future__ import division

from argparse import Namespace

import mock
import pytest

from kafka_utils.kafka_cluster_manager.cluster_info.genetic_balancer \
    import _State
from kafka_utils.kafka_cluster_manager.cluster_info.genetic_balancer \
    import GeneticBalancer


class TestGeneticBalancer(object):

    @pytest.fixture(autouse=True)
    def _create_cluster_topology(self, create_cluster_topology):
        """Make the create_cluster_topology fixture available as
        self.create_cluster_topology.
        """
        self.create_cluster_topology = create_cluster_topology

    def create_balancer(self, cluster_topology=None, **kwargs):
        """Create a GeneticBalancer object."""
        if cluster_topology is None:
            cluster_topology = self.create_cluster_topology()
        args = mock.Mock(spec=Namespace)
        args.max_partition_movements = None
        args.max_movement_size = None
        args.max_leader_changes = None
        args.replication_groups = True
        args.brokers = True
        args.leaders = True
        args.balancer_args = []
        args.configure_mock(**kwargs)
        return GeneticBalancer(cluster_topology, args)

    def move_partition_valid(
            self,
            partition,
            source,
            dest,
            cluster_topology=None,
            **kwargs
    ):
        """Check whether or not a partition movement is allowed."""
        if cluster_topology is None:
            cluster_topology = self.create_cluster_topology()
        balancer = self.create_balancer(cluster_topology, **kwargs)
        state = _State(cluster_topology)
        with mock.patch(
            'kafka_utils.kafka_cluster_manager.cluster_info'
            '.genetic_balancer.random'
        ) as random:
            random.randint.side_effect = [partition, dest]
            random.choice.side_effect = [source]
            # balancer._move_partition returns None on failure.
            return balancer._move_partition(state) is not None

    def move_leadership_valid(
            self,
            partition,
            dest,
            cluster_topology=None,
            **kwargs
    ):
        """Check whether or not a leader movement is allowed."""
        if cluster_topology is None:
            cluster_topology = self.create_cluster_topology()
        balancer = self.create_balancer(cluster_topology, **kwargs)
        state = _State(cluster_topology)
        with mock.patch(
            'kafka_utils.kafka_cluster_manager.cluster_info'
            '.genetic_balancer.random'
        ) as random:
            random.randint.side_effect = [partition, dest]
            return balancer._move_leadership(state) is not None

    def get_scores(self, assignments):
        """Get a list of scores for a given list of assignments"""
        balancer = self.create_balancer()
        return [
            balancer._score(_State(self.create_cluster_topology(assignment)))
            for assignment in assignments
        ]

    def score_lt(self, assignment1, assignment2):
        """Check whether or not assignment1 has a score less than assignment2"""
        scores = self.get_scores([assignment1, assignment2])
        return scores[0] < scores[1]

    def score_eq(self, assignment1, assignment2):
        """Check whether or not assignment1 has a score equal to assignment2"""
        scores = self.get_scores([assignment1, assignment2])
        return scores[0] == scores[1]

    def test_move_partition_valid(self):
        """Test _move_partition for a valid movement.

        Moving partition 0 from broker 1 to broker 4 is allowed
        """
        assert self.move_partition_valid(0, 1, 4)

    def test_move_partition_same_source_dest(self):
        """Test _move_partition when the source and dest brokers are the same.

        Moving partition 0 from broker 1 to broker 1 should be disallowed
        because the destination is the same as the source.
        """
        assert not self.move_partition_valid(0, 1, 1)

    def test_move_partition_rg_imbalanced(self):
        """Test _move_partition when the resulting state is replica-imbalanced.

        Moving partition 0 from broker 1 to broker 3 should be disallowed
        because it causes a replication group imbalance (2 replicas in rg1
        and none in rg2).
        """
        assert not self.move_partition_valid(0, 1, 3)

    def test_move_partition_move_rg(self):
        """Test _move_partition when movement causes a partition to move
        between RGs but resulting state is replica-balanced.

        Moving partition 5 from broker 1 to broker 3 should be allowed
        because, while the movement is from one replication group to another,
        the resulting state is still balanced.
        """
        assert self.move_partition_valid(5, 1, 3)

    def test_move_partition_too_many_movements(self):
        """Test _move_partition when the partition movement limit has been
        reached.

        Moving any partition is invalid because max_partition_movements is 0.
        """
        assert not self.move_partition_valid(5, 1, 3, max_partition_movements=0)

    def test_move_partition_movement_size_too_large(self):
        """Test _move_partition when the partition movement size limit has been
        reached.

        Moving partition 5 is invalid because max_movement_size is 1 which is
        less than the size of the partition (7).
        """
        assert not self.move_partition_valid(5, 1, 3, max_movement_size=1)

    def test_move_partition_too_many_leader_changes_leader(self):
        """Test _move_partition when the leadership change limit has been
        reached and the movement causes a leader change.

        Moving partition 4 from broker 2 is invalid because max_leader_changes
        is 0, but 2 is the leader of partition 4.
        """
        assert not self.move_partition_valid(4, 2, 3, max_leader_changes=0)

    def test_move_partition_too_many_leader_changes_non_leader(self):
        """Test _move_partition when the partition movement size limit has been
        reached and the movement does not cause a leader change.

        Moving partition 5 from broker 1 is valid because, while
        max_leader_changes is 0, broker 1 is not the leader of partition 5.
        """
        assert self.move_partition_valid(5, 1, 4, max_leader_changes=0)

    def test_move_leadership_valid(self):
        """Test _move_leadership for a valid movement.

        Moving the leadership of partition 0 to broker 1 is valid since broker
        1 has a replica of partition 0 and is not already the leader.
        """
        assert self.move_leadership_valid(0, 1)

    def test_move_leadership_no_dest(self):
        """Test _move_leadership when there is no possible destination broker.

        Moving the leadership of partition 4 is invalid since partition 4
        only has one replica so there is no possible destination.
        """
        assert not self.move_leadership_valid(4, 1)

    def test_move_leadership_too_many_leader_changes(self):
        """Test _move_leadership when the leadership change limit has been
        reached.

        Moving leadership is not valid since max_leader_changes is 0.
        """
        assert not self.move_leadership_valid(4, 1, max_leader_changes=0)

    def test_score_broker_weight(self):
        """Test that _score gives a higher score to assignments where the
        partition weight is more balanced across brokers.
        """
        assert self.score_lt({
            (u'T0', 0): ['0'],
            (u'T1', 0): ['0'],
        }, {
            (u'T0', 0): ['0'],
            (u'T1', 0): ['1'],
        })

    def test_score_broker_leader_weight(self):
        """Test that _score gives a higher score to assignments where the
        leader weight is more balanced across brokers.
        """
        assert self.score_lt({
            (u'T0', 0): ['0', '1'],
            (u'T1', 0): ['0', '1'],
        }, {
            (u'T0', 0): ['0', '1'],
            (u'T1', 0): ['1', '0'],
        })

    def test_score_topic_broker_imbalance(self):
        """Test that _score gives a higher score to assignments where topic
        partitions are more distributed across brokers.
        """
        assert self.score_lt({
            (u'T0', 0): ['0', '1'],
            (u'T0', 1): ['1', '0'],
            (u'T1', 0): ['4', '7'],
            (u'T1', 1): ['7', '4'],
        }, {
            (u'T0', 0): ['0', '1'],
            (u'T0', 1): ['4', '7'],
            (u'T1', 0): ['1', '0'],
            (u'T1', 1): ['7', '4'],
        })

    def test_score_balanced_eq(self):
        """Test that _score gives equal score to assignments that are equally
        balanced, but are different.
        """
        assert self.score_eq({
            (u'T0', 0): ['0', '1'],
            (u'T0', 1): ['4', '7'],
            (u'T1', 0): ['1', '0'],
            (u'T1', 1): ['7', '4'],
        }, {
            (u'T0', 0): ['4', '7'],
            (u'T0', 1): ['0', '1'],
            (u'T1', 0): ['7', '4'],
            (u'T1', 1): ['1', '0'],
        })


class Test_State(object):

    @pytest.fixture(autouse=True)
    def _ct(self, create_cluster_topology):
        """Make the default cluster_topology available as self.ct"""
        self.ct = create_cluster_topology()

    @pytest.fixture(autouse=True)
    def _state(self):
        """Make the default state available as self.state"""
        self.state = _State(self.ct)

    def test_partitions(self):
        """Test that partitions are sorted by partition name. This is
        needed to ensure determinism.
        """
        assert self.state.partitions == (
            self.ct.partitions[(u'T0', 0)],
            self.ct.partitions[(u'T0', 1)],
            self.ct.partitions[(u'T1', 0)],
            self.ct.partitions[(u'T1', 1)],
            self.ct.partitions[(u'T2', 0)],
            self.ct.partitions[(u'T3', 0)],
            self.ct.partitions[(u'T3', 1)],
        )

    def test_topics(self):
        """Test that topics are sorted by topic id. This is needed to ensure
        determinism.
        """
        assert self.state.topics == (
            self.ct.topics[u'T0'],
            self.ct.topics[u'T1'],
            self.ct.topics[u'T2'],
            self.ct.topics[u'T3'],
        )

    def test_brokers(self):
        """Test that brokers are sorted by broker id. This is needed to ensure
        determinism.
        """
        assert self.state.brokers == (
            self.ct.brokers['0'],
            self.ct.brokers['1'],
            self.ct.brokers['2'],
            self.ct.brokers['3'],
            self.ct.brokers['4'],
        )

    def test_rgs(self):
        """Test that RGs are sorted by RG id. This is needed to ensure
        determinism.
        """
        assert self.state.rgs == (
            self.ct.rgs['rg1'],
            self.ct.rgs['rg2'],
        )

    def test_replicas(self):
        """Test that the state partition replica map matches the default
        assignment.
        """
        assert self.state.replicas == (
            (1, 2),
            (2, 3),
            (0, 1, 2, 3),
            (0, 1, 2, 3),
            (2,),
            (0, 1, 2),
            (0, 1, 4),
        )

    def test_partition_topic(self):
        """Test that the state partition topic map has been correctly
        initialized.
        """
        assert self.state.partition_topic == (0, 0, 1, 1, 2, 3, 3)

    def test_partition_weights(self):
        """Test that the partition weight map matches the default weights."""
        assert self.state.partition_weights == (2, 3, 4, 5, 6, 7, 8)

    def test_topic_weights(self):
        """Test that the topic weight map has been correctly initialized."""
        assert self.state.topic_weights == (10, 36, 6, 45)

    def test_broker_weights(self):
        """Test that the broker weight map has been correctly initialized."""
        assert self.state.broker_weights == (24, 26, 27, 12, 8)

    def test_broker_leader_weights(self):
        """Test that the broker leader weight map has been correctly
        initialized.
        """
        assert self.state.broker_leader_weights == (24, 2, 9, 0, 0)

    def test_total_weight(self):
        """Test that the total weight has been correctly initialized."""
        assert self.state.total_weight == 97

    def test_partition_sizes(self):
        """Test that the partition size map matches the default sizes."""
        assert self.state.partition_sizes == (3, 4, 5, 6, 7, 8, 9)

    def test_topic_replica_count(self):
        """Test that the topic replica count map has been correctly
        initialized.
        """
        assert self.state.topic_replica_count == (4, 8, 1, 6)

    def test_topic_broker_count(self):
        """Test that the topic broker count map has been correctly
        initialized.
        """
        assert self.state.topic_broker_count == (
            (0, 1, 2, 1, 0),
            (2, 2, 2, 2, 0),
            (0, 0, 1, 0, 0),
            (2, 2, 1, 0, 1),
        )

    def test_topic_broker_imbalance(self):
        """Test that the topic broker imbalance values have been correctly
        initialized.
        """
        assert self.state.topic_broker_imbalance == (1, 1, 0, 1)

    def test_broker_rg(self):
        """Test that the broker RG map has been correctly initialized."""
        assert self.state.broker_rg == (0, 0, 1, 1, 0)

    def test_rg_replicas(self):
        """Test that the RG replica count map has been correctly
        initialized.
        """
        assert self.state.rg_replicas == (
            (1, 0, 2, 2, 0, 2, 3),
            (1, 2, 2, 2, 1, 1, 0),
        )

    def test_assignment(self, default_assignment):
        """Test that the state assignment matches the default_assignment."""
        assert self.state.assignment == default_assignment

    def test_broker_weight_cv(self):
        """Test that broker_weight_cv returns the correct value for the
        default assignment.
        """
        assert abs(self.state.broker_weight_cv - 0.4040) < 1e-4

    def test_broker_leader_weight_cv(self):
        """Test that broker_leader_weight_cv returns the correct value for the
        default assignment.
        """
        assert abs(self.state.broker_leader_weight_cv - 1.3030) < 1e-4

    def test_weighted_topic_broker_imbalance(self):
        """Test that weighted_topic_broker_imbalance returns the correct value
        for the default assignment.
        """
        assert abs(self.state.weighted_topic_broker_imbalance - 91 / 97) < 1e-4

    def test_move_non_leader(self):
        """Test that move returns the correct state for a non-leader movement.

        Move (T1, 1) from 2 (non-leader) to 4.
        """
        new_state = self.state.move(2, 2, 4)

        assert new_state.replicas == (
            (1, 2),
            (2, 3),
            (0, 1, 4, 3),
            (0, 1, 2, 3),
            (2,),
            (0, 1, 2),
            (0, 1, 4),
        )
        assert new_state.broker_weights == (24, 26, 23, 12, 12)
        assert new_state.broker_leader_weights == (24, 2, 9, 0, 0)
        assert new_state.topic_broker_count == (
            (0, 1, 2, 1, 0),
            (2, 2, 1, 2, 1),
            (0, 0, 1, 0, 0),
            (2, 2, 1, 0, 1),
        )
        assert new_state.topic_broker_imbalance == (1, 0, 0, 1)
        assert abs(new_state.broker_weight_cv - 0.3154) < 1e-4
        assert abs(new_state.broker_leader_weight_cv - 1.3030) < 1e-4
        assert abs(new_state.weighted_topic_broker_imbalance - 55 / 97) < 1e-4
        assert new_state.rg_replicas == (
            (1, 0, 3, 2, 0, 2, 3),
            (1, 2, 1, 2, 1, 1, 0),
        )
        assert new_state.movement_count == 1
        assert new_state.movement_size == 5
        assert new_state.leader_movement_count == 0

    def test_move_leader(self):
        """Test that move returns the correct state for a non-leader movement.

        Move (T2, 0) from 2 (the leader) to 3.
        """
        new_state = self.state.move(4, 2, 3)

        assert new_state.replicas == (
            (1, 2),
            (2, 3),
            (0, 1, 2, 3),
            (0, 1, 2, 3),
            (3,),
            (0, 1, 2),
            (0, 1, 4),
        )
        assert new_state.broker_weights == (24, 26, 21, 18, 8)
        assert new_state.broker_leader_weights == (24, 2, 3, 6, 0)
        assert new_state.topic_broker_count == (
            (0, 1, 2, 1, 0),
            (2, 2, 2, 2, 0),
            (0, 0, 0, 1, 0),
            (2, 2, 1, 0, 1),
        )
        assert new_state.topic_broker_imbalance == (1, 1, 0, 1)
        assert abs(new_state.broker_weight_cv - 0.3254) < 1e-4
        assert abs(new_state.broker_leader_weight_cv - 1.2453) < 1e-4
        assert abs(new_state.weighted_topic_broker_imbalance - 91 / 97) < 1e-4
        assert new_state.rg_replicas == (
            (1, 0, 2, 2, 0, 2, 3),
            (1, 2, 2, 2, 1, 1, 0),
        )
        assert new_state.movement_count == 1
        assert new_state.movement_size == 7
        assert new_state.leader_movement_count == 1

    def test_move_multiple(self):
        """Test that move returns the correct state after multiple movements.

        Move (T0, 1) from 2 (leader) to 4.
        Move (T1, 1) from 1 (non-leader) to 4.
        Move (T3, 1) from 0 (leader) to 3.
        """
        new_state = self.state.move(1, 2, 4).move(3, 1, 4).move(6, 0, 3)
        assert new_state.replicas == (
            (1, 2),
            (4, 3),
            (0, 1, 2, 3),
            (0, 4, 2, 3),
            (2,),
            (0, 1, 2),
            (3, 1, 4),
        )
        assert new_state.broker_weights == (16, 21, 24, 20, 16)
        assert new_state.broker_leader_weights == (16, 2, 6, 8, 3)
        assert new_state.topic_broker_count == (
            (0, 1, 1, 1, 1),
            (2, 1, 2, 2, 1),
            (0, 0, 1, 0, 0),
            (1, 2, 1, 1, 1),
        )
        assert new_state.topic_broker_imbalance == (0, 0, 0, 0)
        assert abs(new_state.broker_weight_cv - 0.1584) < 1e-4
        assert abs(new_state.broker_leader_weight_cv - 0.7114) < 1e-4
        assert new_state.weighted_topic_broker_imbalance == 0
        assert new_state.rg_replicas == (
            (1, 1, 2, 2, 0, 2, 2),
            (1, 1, 2, 2, 1, 1, 1),
        )
        assert new_state.movement_count == 3
        assert new_state.movement_size == 19
        assert new_state.leader_movement_count == 2

    def test_move_leadership(self):
        """Test that move_leadership returns the correct state.

        Move (T1, 1) leadership from 0 to 2.
        """
        new_state = self.state.move_leadership(3, 2)

        assert new_state.replicas == (
            (1, 2),
            (2, 3),
            (0, 1, 2, 3),
            (2, 1, 0, 3),
            (2,),
            (0, 1, 2),
            (0, 1, 4),
        )
        assert new_state.broker_leader_weights == (19, 2, 14, 0, 0)
        assert abs(new_state.broker_leader_weight_cv - 1.1357) < 1e-4
        assert new_state.rg_replicas == (
            (1, 0, 2, 2, 0, 2, 3),
            (1, 2, 2, 2, 1, 1, 0),
        )
        assert new_state.movement_count == 0
        assert new_state.movement_size == 0
        assert new_state.leader_movement_count == 1

    def test_add_replica(self):
        """Test that add_replica returns the correct state.

        Add one replica of (T2, 0) to 4.
        """
        new_state = self.state.add_replica(4, 4)

        assert new_state.replicas == (
            (1, 2),
            (2, 3),
            (0, 1, 2, 3),
            (0, 1, 2, 3),
            (2, 4),
            (0, 1, 2),
            (0, 1, 4),
        )
        assert new_state.broker_weights == (24, 26, 27, 12, 14)
        assert new_state.broker_leader_weights == (24, 2, 9, 0, 0)
        assert new_state.topic_broker_count == (
            (0, 1, 2, 1, 0),
            (2, 2, 2, 2, 0),
            (0, 0, 1, 0, 1),
            (2, 2, 1, 0, 1),
        )
        assert new_state.topic_broker_imbalance == (1, 1, 0, 1)
        assert abs(new_state.broker_weight_cv - 0.3064) < 1e-4
        assert abs(new_state.broker_leader_weight_cv - 1.3030) < 1e-4
        assert abs(new_state.weighted_topic_broker_imbalance - 91 / 103) < 1e-4
        assert new_state.rg_replicas == (
            (1, 0, 2, 2, 1, 2, 3),
            (1, 2, 2, 2, 1, 1, 0),
        )
        assert new_state.movement_count == 0
        assert new_state.movement_size == 0
        assert new_state.leader_movement_count == 0

    def test_remove_replica(self):
        """Test that remove_replica returns the correct state.

        Remove one replica of (T3, 0) from 2.
        """
        new_state = self.state.remove_replica(5, 2)

        assert new_state.replicas == (
            (1, 2),
            (2, 3),
            (0, 1, 2, 3),
            (0, 1, 2, 3),
            (2,),
            (0, 1),
            (0, 1, 4),
        )
        assert new_state.broker_weights == (24, 26, 20, 12, 8)
        assert new_state.broker_leader_weights == (24, 2, 9, 0, 0)
        assert new_state.topic_broker_count == (
            (0, 1, 2, 1, 0),
            (2, 2, 2, 2, 0),
            (0, 0, 1, 0, 0),
            (2, 2, 0, 0, 1),
        )
        assert new_state.topic_broker_imbalance == (1, 1, 0, 2)
        assert abs(new_state.broker_weight_cv - 0.3849) < 1e-4
        assert abs(new_state.broker_leader_weight_cv - 1.3030) < 1e-4
        assert abs(new_state.weighted_topic_broker_imbalance - 122 / 90) < 1e-4
        assert new_state.rg_replicas == (
            (1, 0, 2, 2, 0, 2, 3),
            (1, 2, 2, 2, 1, 0, 0),
        )
        assert new_state.movement_count == 0
        assert new_state.movement_size == 0
        assert new_state.leader_movement_count == 0
