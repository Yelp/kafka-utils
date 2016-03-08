from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os
import subprocess
from collections import OrderedDict


def get_plan(proposed_assignment):
    return {
        'version': 1,
        'partitions':
        [{'topic': t_p[0],
          'partition': t_p[1],
          'replicas': replica
          } for t_p, replica in proposed_assignment.iteritems()]
    }


class KafkaInterface(object):
    """This class acts as an interface to interact with kafka-scripts."""

    def __init__(self, script_path=None):
        self._kafka_script_path = script_path
        self.log = logging.getLogger(self.__class__.__name__)

    def run_repartition_cmd(
        self,
        zookeeper,
        brokers,
        temp_file_name,
        is_execute=False,
    ):
        """Run reassignment kafka command based on given parameters.

        Keywords:
        temp_file_name: File containing the topic list
                        or topic-partition layout over brokers,
                        which helps reassignment script to
                        generate or execute the plan.
        is_execute:     Flag symbolizing generation or execution of plan.
        return:         Generated and Proposed plan or execution status based on
                        whether command is for generation
                        or execution respectively.
        """
        cmd_params = {}
        if is_execute:
            cmd_params['action'] = '--execute'
            cmd_params['file_flag'] = '--reassignment-json-file'
        else:
            cmd_params['action'] = '--generate'
            cmd_params['file_flag'] = '--topics-to-move-json-file'
        brokers_str = ','.join(str(e) for e in brokers)
        reassignment_path = os.path.join(
            os.path.abspath(self._kafka_script_path),
            'kafka-reassign-partitions.sh',
        )
        cmd = [
            reassignment_path,
            cmd_params['action'],
            '--zookeeper',
            zookeeper,
            cmd_params['file_flag'],
            temp_file_name,
            '--broker-list',
            brokers_str,
        ]
        return subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()

    def get_cluster_assignment(self, zk, brokers, topic_ids):
        """Generate the reassignment plan for given zookeeper
        configuration, brokers and topics.
        """
        plan = zk.get_cluster_plan()
        assignment = {}
        for elem in plan['partitions']:
            assignment[
                (elem['topic'], elem['partition'])
            ] = elem['replicas']
        # assignment map created in sorted order for deterministic solution
        assignment = OrderedDict(sorted(assignment.items(), key=lambda t: t[0]))
        return assignment

    def execute_plan(self, zk, plan):
        """Execute the proposed plan.

        Execute the given proposed plan over given
        brokers and zookeeper configuration

        Arguments:
        proposed_plan:   Proposed plan in json format
        """
        return zk.execute_plan(plan)
