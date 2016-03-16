from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from collections import OrderedDict


def assignment_to_plan(assignment):
    """Convert an assignment to the format used by Kafka to describe a reassignment plan"""
    return {
        'version': 1,
        'partitions':
        [{'topic': t_p[0],
          'partition': t_p[1],
          'replicas': replica
          } for t_p, replica in assignment.iteritems()]
    }


class KafkaInterface(object):
    """This class acts as an interface to interact with Kafka."""

    def __init__(self, script_path=None):
        self._kafka_script_path = script_path
        self.log = logging.getLogger(self.__class__.__name__)

    def get_cluster_assignment(self, zk):
        """Generate the reassignment plan for given zookeeper
        configuration.
        """
        plan = zk.get_cluster_plan()
        assignment = {}
        for elem in plan['partitions']:
            assignment[
                (elem['topic'], elem['partition'])
            ] = elem['replicas']
        # assignment map created in sorted order for deterministic solution
        return OrderedDict(sorted(assignment.items(), key=lambda t: t[0]))

    def execute_plan(self, zk, plan):
        """Execute the proposed plan.

        Execute the given proposed plan over given
        zookeeper configuration

        Arguments:
        plan:   Proposed plan in json format
        """
        return zk.execute_plan(plan)
