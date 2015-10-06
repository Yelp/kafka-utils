from __future__ import print_function

import json
import sys

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError
from yelp_kafka_tool.util import config

REASSIGNMENT_ZOOKEEPER_PATH = "/admin/reassign_partitions"


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

    def get_topics(self, topic_name=None, names_only=False):
        """Get information on all the available topics."""
        topic_ids = [topic_name] if topic_name else self.get_children(
            "/brokers/topics"
        )
        if names_only:
            return topic_ids
        try:
            topics = [
                self.get("/brokers/topics/{id}".format(id=id))
                for id in topic_ids
            ]
        except NoNodeError:
            print(
                "[ERROR] topic '{topic}' not found.".format(topic=topic_name),
                file=sys.stderr,
            )
            return {}
        result = {}
        state_path = "/brokers/topics/{topic_id}/partitions/{p_id}/state"
        for topic_id, [topic_json, _] in zip(topic_ids, topics):
            topic = json.loads(topic_json)
            partitions = topic["partitions"]
            partitions_data = {}
            for p_id, replicas in partitions.items():
                try:
                    partition_json, _ = self.get(
                        state_path.format(topic_id=topic_id, p_id=p_id)
                    )
                    partitions_data[p_id] = json.loads(partition_json)
                    partitions_data[p_id]['replicas'] = replicas
                except NoNodeError:
                    partitions_data[p_id] = None  # The partition has no data
            topic['partitions'] = partitions_data
            result[topic_id] = topic
        return result

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
        :param: recrusive: Recursively delete node and all its children.
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

    def execute_assignment(self, data):
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
        path = REASSIGNMENT_ZOOKEEPER_PATH
        plan = json.dumps(data)
        try:
            print('[INFO] Sending assignment to Zookeeper...')
            self.create(path, plan, makepath=True)
            print('[INFO] Assignment sent to Zookeeper successfully.')
            # TODO: Read node to list data of currently running??
        except NodeExistsError:
            print('[ERROR] Previous assignment in progress. Exiting..')
        except Exception as e:
            print(
                '[ERROR] Could not re-assign partitions {plan}. Error: {e}'
                .format(plan=plan, e=e),
            )
            raise

    def get_cluster_assignment(self):
        """Fetch cluster assignment directly from zookeeper."""
        cluster_layout = self.get_partitions()
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
