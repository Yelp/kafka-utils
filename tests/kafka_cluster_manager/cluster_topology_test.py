import contextlib
from collections import OrderedDict
from mock import sentinel, patch, MagicMock
from pytest import fixture

from yelp_kafka_tool.kafka_cluster_manager.cluster_info.cluster_topology import (
    ClusterTopology,
)
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.stats import (
    get_replication_group_imbalance_stats,
)
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.util import (
    compute_optimal_count,
)
from yelp_kafka_tool.kafka_cluster_manager.main import ZK
from yelp_kafka.config import ClusterConfig


class TestClusterToplogy(object):
    broker_rg = {0: 'rg1', 1: 'rg1', 2: 'rg2', 3: 'rg2', 4: 'rg1', 5: 'rg3'}
    topic_ids = ['T0', 'T1', 'T2', 'T3']
    brokers_info = {
        '0': sentinel.obj1,
        '1': sentinel.obj2,
        '2': sentinel.obj3,
        '3': sentinel.obj4,
        '4': sentinel.obj5,
    }
    # Example assignment properties:
    # * Brokers:(0,1,2,3): rg-count = 2
    # case 1: repl-factor % rg-count == 0
    #    -- T0, T1:
    #   * 1a) T1: repl-factor > rg-count
    # case 2: repl-factor % rg-count != 0
    #   -- T2, T3
    #   * 2a): repl-factor > rg-count: T1
    #   * 2b): repl-factor < rg-count: T2
    # rg-imbalance-status per partition:
    #
    # rg-imbalanced-partitions: T0-1, T1-1, T3-1
    # rg-balanced-partitions:   T0-0, T1-0, T3-0, T2-0
    _initial_assignment = OrderedDict(
        [
            ((u'T0', 0), [1, 2]),
            ((u'T0', 1), [2, 3]),
            ((u'T1', 0), [0, 1, 2, 3]),
            ((u'T1', 1), [0, 1, 2, 4]),
            ((u'T2', 0), [2]),
            ((u'T3', 0), [0, 1, 2]),
            ((u'T3', 1), [0, 1, 4]),
        ]
    )

    @fixture
    def mock_zk(self):
        # Mock zookeeper
        mock_cluster_config = MagicMock(spec=ClusterConfig)
        mock_cluster_config.name = "test-config"
        mock_zk = MagicMock(spec=ZK, cluster_config=mock_cluster_config)
        mock_zk.get_brokers.return_value = self.brokers_info
        mock_zk.get_topics.return_value = self.topic_ids
        return mock_zk

    @contextlib.contextmanager
    def mock_get_replication_group_id(self):
        with patch.object(
            ClusterTopology,
            "_get_replication_group_id",
            spec=ClusterTopology._get_replication_group_id,
            side_effect=self.get_replication_group_id,
        ) as mock_get_replication_group_id:
            yield mock_get_replication_group_id

    def get_replication_group_id(self, broker):
        return self.broker_rg[broker.id]

    @contextlib.contextmanager
    def build_cluster_topology(self, assignment=None, broker_ids=None):
        """Create cluster topology from given assignment."""
        if not assignment:
            assignment = self._initial_assignment
        mock_cluster_config = MagicMock(spec=ClusterConfig)
        mock_cluster_config.name = "test-config"
        mock_zk = MagicMock(spec=ZK, cluster_config=mock_cluster_config)
        if broker_ids:
            brokers_info = {
                broker_id: (sentinel.obj, broker_id)
                for broker_id in broker_ids
            }
        else:
            brokers_info = self.brokers_info
        topic_ids = sorted(set([t_p[0] for t_p in assignment.iterkeys()]))
        mock_zk.get_brokers.return_value = brokers_info
        mock_zk.get_topics.return_value = topic_ids
        with contextlib.nested(
            patch(
                'yelp_kafka_tool.kafka_cluster_manager.cluster_info.'
                'cluster_topology.KafkaInterface',
                autospec=True,
            ),
            self.mock_get_replication_group_id(),
        ) as (mock_kafka, mock_rg_groups):
            # Create cluster-object
            mock_kafka.return_value.get_cluster_assignment.return_value = assignment
            yield ClusterTopology(mock_zk)

    def assert_valid(self, new_assignment, orig_assignment, orig_brokers):
        """Assert if new-assignment is valid based on given assignment.

        Asserts the results for following parameters:
        a) Asserts that keys in both assignments are same
        b) Asserts that replication-factor of result remains same
        c) Assert that new-replica-brokers are amongst given broker-list
        """
        # Asserts that partitions remain same
        assert sorted(orig_assignment.keys()) == sorted(new_assignment.keys())
        for t_p, new_replicas in new_assignment.iteritems():
            orig_replicas = orig_assignment[t_p]
            # Assert that new-replias are amongst given broker-list
            assert(all([broker in orig_brokers for broker in new_replicas]))
            # Assert that replication-factor remains same
            assert(len(new_replicas) == len(orig_replicas))

    def get_partition_data(self, ct, p_name):
        partition = ct.partitions[p_name]
        opt_cnt, extra_cnt = \
            compute_optimal_count(
                partition.replication_factor,
                len(ct.rgs.values()),
            )
        return partition, opt_cnt, not extra_cnt

    def test_rebalance_replication_groups(self):
        with self.build_cluster_topology() as ct:
            ct.rebalance_replication_groups()
            net_imbal, extra_cnt_per_rg = get_replication_group_imbalance_stats(
                ct.rgs.values(),
                ct.partitions.values(),
            )

            # Verify that rg-group-balanced
            assert(net_imbal, 0)

            # Verify that new-assignment is valid
            self.assert_valid(
                ct.assignment,
                self._initial_assignment,
                ct.brokers.keys(),
            )

    def test_rebalance_replication_groups_balanced(self):
        # Replication-group is already balanced
        assignment = OrderedDict(
            [
                ((u'T0', 0), [0, 2]),
                ((u'T0', 1), [0, 3]),
                ((u'T2', 0), [2]),
                ((u'T3', 0), [0, 1, 2]),
            ]
        )
        with self.build_cluster_topology(assignment, ['0', '1', '2', '3', '4']) as ct:
            ct.reassign_partitions()
            net_imbal, extra_cnt_per_rg = get_replication_group_imbalance_stats(
                ct.rgs.values(),
                ct.partitions.values(),
            )

            # Verify that rg-group-balanced
            assert net_imbal == 0

            # Verify that new-assignment same as previous
            assert ct.assignment == assignment

    def test_segregate_replication_groups_case1(self):
        """Test segregation of replication-groups based on under
        or over-replicated partitions.

        Case 1: repl-factor % rg-count == 0
        """
        with self.build_cluster_topology() as ct:
            # Partition: T0-0
            partition, opt_cnt, evenly_distribute = self.get_partition_data(ct, ('T0', 0))
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(
                    partition,
                    opt_cnt,
                    evenly_distribute,
                )
            # Tests compute-optimal-count
            assert opt_cnt == 1 and evenly_distribute is True
            # Tests segregation of rg's
            assert not (over_replicated_rgs or under_replicated_rgs)

            # Partition T0-1: # Already-balanced
            # opt-count == 1
            partition, opt_cnt, evenly_distribute = self.get_partition_data(ct, ('T0', 1))
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(
                    partition,
                    opt_cnt,
                    evenly_distribute,
                )
            assert opt_cnt == 1 and evenly_distribute is True
            under_rg_ids = [rg.id for rg in under_replicated_rgs]
            over_rg_ids = [rg.id for rg in over_replicated_rgs]
            assert (over_rg_ids == ['rg2']) and (under_rg_ids == ['rg1'])

            # Partition: T1-0 # Already-balanced: repl-factor > rg-count
            # opt-count > 1
            partition, opt_cnt, evenly_distribute = self.get_partition_data(ct, ('T1', 0))
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(
                    partition,
                    opt_cnt,
                    evenly_distribute,
                )
            assert opt_cnt == 2 and evenly_distribute is True
            assert not (over_replicated_rgs or under_replicated_rgs)

    def test_segregate_replication_groups_case_2(self):
        """Test segregation of replication-groups based on under
        or over-replicated partitions.

        Case 2: repl-factor % rg-count != 0
        """

        with self.build_cluster_topology() as ct:
            # Partition: T2-0: repl-factor < rg-count
            # opt-count:  0
            partition, opt_cnt, evenly_distribute = self.get_partition_data(ct, ('T2', 0))
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(
                    partition,
                    opt_cnt,
                    evenly_distribute,
                )
            under_rg_ids = [rg.id for rg in under_replicated_rgs]
            over_rg_ids = [rg.id for rg in over_replicated_rgs]
            # Tests compute-optimal-count
            assert opt_cnt == 0 and evenly_distribute is False
            # Tests segregation of rg's
            assert (over_rg_ids == ['rg2']) and (under_rg_ids == ['rg1'])

            # Partition T3-0: # Already-balanced
            # opt-count: 1
            partition, opt_cnt, evenly_distribute = self.get_partition_data(ct, ('T3', 0))
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(
                    partition,
                    opt_cnt,
                    evenly_distribute,
                )
            under_rg_ids = [rg.id for rg in under_replicated_rgs]
            over_rg_ids = [rg.id for rg in over_replicated_rgs]
            assert opt_cnt == 1 and evenly_distribute is False
            assert (under_rg_ids == ['rg2'])and over_rg_ids == ['rg1']

            # Partition: T3-1 # not-balanced: repl-factor > rg-count
            # opt-count > 1
            partition, opt_cnt, evenly_distribute = self.get_partition_data(ct, ('T3', 1))
            under_replicated_rgs, over_replicated_rgs = \
                ct._segregate_replication_groups(
                    partition,
                    opt_cnt,
                    evenly_distribute,
                )
            under_rg_ids = [rg.id for rg in under_replicated_rgs]
            over_rg_ids = [rg.id for rg in over_replicated_rgs]
            assert opt_cnt == 1 and evenly_distribute is False
            assert (under_rg_ids == ['rg2'])and over_rg_ids == ['rg1']
