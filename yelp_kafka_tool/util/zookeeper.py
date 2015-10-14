from __future__ import print_function

import json
import logging
import sys

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from yelp_kafka_tool.util import config
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.util import (
    validate_plan,
)


REASSIGNMENT_ZOOKEEPER_PATH = "/admin/reassign_partitions"
ADMIN_PATH = "/admin"
REASSIGNMENT_NODE = "reassign_partitions"
_log = logging.getLogger('kafka-zookeeper-manager')


class ZK:
    """Opens a connection to a kafka zookeeper. "
    "To be used in the 'with' statement."""

    def __init__(self, cluster_config):
        self.cluster_config = cluster_config

    def __enter__(self):
        self.zk = KazooClient(
            hosts=self.cluster_config.zookeeper,
            read_only=True,
        )
        if config.debug:
            print(
                "[INFO] ZK: Creating new zookeeper connection: {zookeeper}"
                .format(zookeeper=self.cluster_config.zookeeper),
                file=sys.stderr
            )
        self.zk.start()
        return self

    def __exit__(self, type, value, traceback):
        self.zk.stop()

    def get_children(self, path, watch=None):
        """Returns the children of the specified node."""
        if config.debug:
            print(
                "[INFO] ZK: Getting children of {path}".format(path=path),
                file=sys.stderr,
            )
        return self.zk.get_children(path, watch)

    def get(self, path, watch=None):
        """Returns the data of the specified node."""
        if config.debug:
            print(
                "[INFO] ZK: Getting {path}".format(path=path),
                file=sys.stderr,
            )
        return self.zk.get(path, watch)

    def get_json(self, path, watch=None):
        """Reads the data of the specified node and converts it to json."""
        data, _ = self.get(path, watch)
        return json.loads(data) if data else None

    def get_brokers(self, broker_name=None):
        """Get information on all the available brokers.

        :rtype : dict of brokers
        """
        if broker_name is not None:
            broker_ids = [broker_name]
        else:
            broker_ids = self.get_children("/brokers/ids")
        brokers = {}
        for b_id in broker_ids:
            try:
                broker_json, _ = self.get(
                    "/brokers/ids/{b_id}".format(b_id=b_id)
                )
            except NoNodeError:
                print(
                    "[ERROR] broker '{b_id}' not found.".format(b_id=b_id),
                    file=sys.stderr
                )
                sys.exit(1)
            broker = json.loads(broker_json)
            brokers[b_id] = broker
        return brokers

    def get_topics(
        self,
        topic_name=None,
        names_only=False,
        fetch_partition_state=True,
    ):
        """Get information on all the available topics.

        Topic-data format with fetch_partition_state as False :-
        topic_data = {
            'version': 1,
            'partitions': {
                <p_id>: {
                    replicas: <broker-ids>
                }
            }
        }

        Topic-data format with fetch_partition_state as True:-
        topic_data = {
            'version': 1,
            'partitions': {
                <p_id>:{
                    replicas: [<broker_id>, <broker_id>, ...],
                    isr: [<broker_id>, <broker_id>, ...],
                    controller_epoch: <val>,
                    leader_epoch: <val>,
                    version: 1,
                    leader: <broker-id>,
            }
        }
        Note: By default we also fetch partition-state which results in
        accessing the zookeeper twice. If just partition-replica information is
        required fetch_partition_state should be set to False.
        """
        topic_ids = [topic_name] if topic_name else self.get_children(
            "/brokers/topics",
        )
        if names_only:
            return topic_ids
        topics_data = {}
        for topic_id in topic_ids:
            try:
                topic_data = json.loads(
                    self.get("/brokers/topics/{id}".format(id=topic_id))[0],
                )
            except NoNodeError:
                print(
                    "[ERROR] topic '{topic}' not found.".format(topic=topic_id),
                    file=sys.stderr,
                )
                return {}
            # Prepare data for each partition
            partitions_data = {}
            for p_id, replicas in topic_data['partitions'].iteritems():
                partitions_data[p_id] = {}
                if fetch_partition_state:
                    # Fetch partition-state from zookeeper
                    partitions_data[p_id] = self._fetch_partition_state(topic_id, p_id)
                partitions_data[p_id]['replicas'] = replicas
            topic_data['partitions'] = partitions_data
            topics_data[topic_id] = topic_data
        return topics_data

    def _fetch_partition_state(self, topic_id, partition_id):
        """Fetch partition-state for given topic-partition."""
        state_path = "/brokers/topics/{topic_id}/partitions/{p_id}/state"
        try:
            partition_json, _ = self.get(
                state_path.format(topic_id=topic_id, p_id=partition_id),
            )
            return json.loads(partition_json)
        except NoNodeError:
            return {}  # The partition has no data

    def get_my_subscribed_topics(self, groupid):
        """Get the list of topics that a consumer is subscribed to

        :param: groupid: The consumer group ID for the consumer
        :returns list of kafka topics
        :rtype: list
        """
        path = "/consumers/{group_id}/offsets".format(group_id=groupid)
        return self.get_children(path)

    def get_my_subscribed_partitions(self, groupid, topic):
        """Get the list of partitions of a topic
        that a consumer is subscribed to

        :param: groupid: The consumer group ID for the consumer
        :param: topic: The topic name
        :returns list of partitions
        :rtype: list
        """
        path = "/consumers/{group_id}/offsets/{topic}".format(
            group_id=groupid,
            topic=topic,
        )
        return self.get_children(path)

    def create(
        self,
        path,
        value='',
        acl=None,
        ephemeral=False,
        sequence=False,
        makepath=False
    ):
        """Creates a Zookeeper node.

        :param: path: The zookeeper node path
        :param: value: Zookeeper node value
        :param: acl: ACL list
        :param: ephemeral: Boolean indicating where this node is tied to
          this session.
        :param: sequence:  Boolean indicating whether path is suffixed
          with a unique index.
        :param: makepath: Whether the path should be created if it doesn't
          exist.
        """
        if config.debug:
            print("[INFO] ZK: Creating node " + path, file=sys.stderr)
        return self.zk.create(path, value, acl, ephemeral, sequence, makepath)

    def delete(self, path, recursive=False):
        """Deletes a Zookeeper node.

        :param: path: The zookeeper node path
        :param: recursive: Recursively delete node and all its children.
        """
        if config.debug:
            print("[INFO] ZK: Deleting node " + path, file=sys.stderr)
        return self.zk.delete(path, recursive=recursive)

    def delete_topic_partitions(self, groupid, topic, partitions):
        """Delete the specified partitions within the topic that the consumer
        is subscribed to.

        :param: groupid: The consumer group ID for the consumer.
        :param: topic: Kafka topic.
        :param: partitions: List of partitions within the topic to be deleted.
        :raises:
          NoNodeError: if the consumer is not subscribed to the topic

          ZookeeperError: if there is an error with Zookeeper
        """
        for partition in partitions:
            path = "/consumers/{groupid}/offsets/{topic}/{partition}".format(
                groupid=groupid,
                topic=topic,
                partition=partition
            )
            self.delete(path)

    def delete_topic(self, groupid, topic):
        path = "/consumers/{groupid}/offsets/{topic}".format(
            groupid=groupid,
            topic=topic,
        )
        self.delete(path)

    def execute_assignment(self, assignment):
        """Executing plan directly sending it to zookeeper nodes.
        Algorithm:
        1. Verification:
         a) TODO: next review: Validate given assignment
        2. TODO:Save current assignment for future?
        3. Re-assign:
            * Send command to zookeeper to re-assign and create parent-node
              if missing.
            Exceptions:
            * NodeExists error: Assignment already in progress
            * Raise any other exception

        """
        path = REASSIGNMENT_ZOOKEEPER_PATH
        plan = json.dumps(assignment)
        # Verify if previous assignment is in progress
        if REASSIGNMENT_NODE in self.get_children(ADMIN_PATH):
            try:
                in_progress_assignment = json.loads(self.get(path)[0])
                in_progress_partitions = [
                    '{topic}-{p_id}'.format(
                        topic=p_data['topic'],
                        p_id=str(p_data['partition']),
                    )
                    for p_data in in_progress_assignment['partitions']
                ]
                _log.error(
                    '{count} partition(s) reassignment currently in progress:-'
                    .format(count=len(in_progress_partitions)),
                )
                _log.error(
                    '{partitions}. ABORTING reassignment...'
                    .format(partitions=', '.join(in_progress_partitions)),
                )
                return
            except Exception as e:
                _log.error(
                    'Information in_{path} could not be parsed. ABORTING '
                    'reassignment...'.format(path=path),
                )
        # Verify if given plan is valid plan
        # Fetch latest assignment from zookeeper
        base_assignment = self.get_cluster_assignment()
        if not validate_plan(assignment, base_assignment):
            _log.error('Given plan is invalid. ABORTING reassignment...')
            return
        # Execute assignment
        try:
            _log.info('Sending assignment to Zookeeper...')
            self.create(path, plan, makepath=True)
            _log.info('Assignment sent to Zookeeper successfully.')
        except Exception as e:
            _log.error(
                'Could not re-assign partitions {plan}. Error: {e}'
                .format(plan=plan, e=e),
            )
            raise

    def get_cluster_assignment(self):
        """Fetch cluster assignment directly from zookeeper."""
        _log.info('Fetching current cluster-topology from Zookeeper...')
        cluster_layout = self.get_topics(fetch_partition_state=False)
        # Re-format cluster-layout
        partitions = [
            {
                'topic': topic_id,
                'partition': int(p_id),
                'replicas': partitions_data['replicas']
            }
            for topic_id, topic_info in cluster_layout.iteritems()
            for p_id, partitions_data in topic_info['partitions'].iteritems()
        ]
        return {
            'version': 1,
            'partitions': partitions
        }
