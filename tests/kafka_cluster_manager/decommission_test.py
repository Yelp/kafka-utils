from __future__ import unicode_literals

from argparse import Namespace

import mock
import pytest

from kafka_utils.kafka_cluster_manager.cluster_info \
    .partition_count_balancer import PartitionCountBalancer
from kafka_utils.kafka_cluster_manager.cmds import decommission
from tests.kafka_cluster_manager.helper import broker_range


@pytest.fixture
def command_instance():
    cmd = decommission.DecommissionCmd()
    cmd.args = mock.Mock(spec=Namespace)
    cmd.args.force_progress = False
    cmd.args.broker_ids = []
    cmd.args.auto_max_movement_size = True
    return cmd


def test_decommission_no_partitions_to_move(command_instance, create_cluster_topology):
    cluster_one_broker_empty = create_cluster_topology(
        assignment={('topic', 0): [0, 1]},
        brokers=broker_range(3),
    )
    command_instance.args.brokers_ids = [2]
    balancer = PartitionCountBalancer(cluster_one_broker_empty, command_instance.args)
    command_instance.run_command(cluster_one_broker_empty, balancer)
