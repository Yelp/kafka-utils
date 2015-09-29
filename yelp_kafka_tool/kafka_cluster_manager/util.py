from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import json
import os
import subprocess
import tempfile

from kazoo.exceptions import NodeExistsError, NoNodeError
from .cluster_info.util import get_assignment_map

REASSIGNMENT_ZOOKEEPER_PATH = "/admin/reassign_partitions"
REASSIGNMENT_PARENT_PATH = "/admin"


class KafkaInterface(object):
    """This class acts as an interface to interact with kafka-scripts."""

    def __init__(self, script_path=None):
        self._kafka_script_path = script_path
        self.log = logging.getLogger(self.__class__.__name__)
        self.reassignment_zookeeper_path = REASSIGNMENT_ZOOKEEPER_PATH

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
            brokers_str
        ]
        """Run reassignment kafka command based on given parameters."""
        return subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()

    def get_cluster_assignment(self, zk, brokers, topic_ids):
        """Generate the reassignment plan for given zookeeper
        configuration, brokers and topics.
        """
        topic_data = {
            'topics': [{'topic': topic_id} for topic_id in topic_ids],
            'version': 1
        }
        if not self._kafka_script_path:
            assignment = self.get_cluster_assignment_zk(zk)
            return get_assignment_map(assignment)
        else:
            zookeeper = zk.cluster_config.zookeeper
            with tempfile.NamedTemporaryFile() as temp_topic_file:
                temp_topic_file.write(json.dumps(topic_data))
                temp_topic_file.flush()
                result = self.run_repartition_cmd(
                    zookeeper,
                    brokers,
                    temp_topic_file.name
                )
                try:
                    json_result_list = result[0].split('\n')
                    curr_layout = json.loads(json_result_list[-5])
                    if curr_layout:
                        return get_assignment_map(curr_layout)
                    else:
                        raise ValueError(
                            'Output: {output} Plan Generation Error: {error}'
                            .format(output=result[0], error=result[1])
                        )
                except ValueError as error:
                    self.log.error('%s', error)
                    raise ValueError(
                        'Could not parse output of kafka-executable script %s',
                        result,
                    )

    def execute_plan(
        self,
        proposed_layout,
        zk,
        brokers,
        topics,
    ):
        """Execute the proposed plan.

        Execute the given proposed plan over given
        brokers and zookeeper configuration

        Arguments:
        proposed_plan:   Proposed plan in json format
        """

        if not self._kafka_script_path:
            self.execute_assignment_zk(zk, proposed_layout)
        else:
            with tempfile.NamedTemporaryFile() as temp_reassignment_file:
                json.dump(proposed_layout, temp_reassignment_file)
                temp_reassignment_file.flush()
                zookeeper = zk.cluster_config.zookeeper
                self.run_repartition_cmd(
                    zookeeper,
                    brokers,
                    temp_reassignment_file.name,
                    True,
                )

    def execute_assignment_zk(self, zk, data):
        """Executing plan directly sending it to zookeeper nodes.
        Algorithm:
        1. Verification:
         a) Verify that data is not empty
         b) Verify no duplicate partitions
        2. Save current assignment for future (save, skipping)
        3. Verify if partitions exist  (skipping)
            Throw partition-topic not exist error
        4. Re-assign:
            Exceptions:
            * NodeExists error: Assignment already in progress
                -- Get partitions which are in progress
            * NoNode error: create parent node
            * Raise any other exception throw

        """
        path = self.reassignment_zookeeper_path
        plan = json.dumps(data)
        try:
            self.log.info('Sending assignment to Zookeeper...')
            zk.create(path, plan)
            self.log.info('Assignment sent to Zookeeper successfully.')
        except NodeExistsError:
            # If reassign_partitions node already exists, report
            self.log.error(
                'Sending assignment to zookeeper Failed. Previous_assignment'
                ' currently in progress...',
            )
            # TODO: Read node to list data of currently running??
        except NoNodeError:
            parent = REASSIGNMENT_PARENT_PATH
            self.log.warning('Admin node missing in zookeeper, creating admin path... ')
            zk.create(parent, makepath=True)
            self.log.info('Second attempt: Sending assignment to Zookeeper...')
            zk.create(path, plan)
            self.log.info('Assignment sent to Zookeeper successfully.')
        except exception as e:
            self.log.error(
                'Could not re-assign partitions {plan}. Error: {e}'
                .format(plan=plan, e=e),
            )
            raise

    def get_cluster_assignment_zk(self, zk):
        """Fetch cluster assignment directly from zookeeper."""
        cluster_layout = zk.get_partitions()
        # Re-format cluster-layout
        partitions = [
            {
                'topic': topic_id,
                'partition': int(p_id),
                'replicas': replicas
            }
            for topic_id, topic_info in cluster_layout.iteritems()
            for p_id, replicas in topic_info['partitions'].iteritems()
        ]
        return {
            'version': 1,
            'partitions': partitions
        }
