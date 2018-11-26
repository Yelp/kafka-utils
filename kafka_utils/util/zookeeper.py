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

import logging

import six
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from kazoo.exceptions import NoNodeError
from kazoo.retry import KazooRetry

from kafka_utils.util.serialization import dump_json
from kafka_utils.util.serialization import load_json
from kafka_utils.util.validation import validate_plan


ADMIN_PATH = "/admin"
REASSIGNMENT_NODE = "reassign_partitions"
_log = logging.getLogger('kafka-zookeeper-manager')


class ZK:
    """Opens a connection to a kafka zookeeper. "
    "To be used in the 'with' statement."""

    def __init__(self, cluster_config):
        self.cluster_config = cluster_config

    def __enter__(self):
        kazooRetry = KazooRetry(
            max_tries=5,
        )
        self.zk = KazooClient(
            hosts=self.cluster_config.zookeeper,
            read_only=True,
            connection_retry=kazooRetry,
        )
        _log.debug(
            "ZK: Creating new zookeeper connection: {zookeeper}"
            .format(zookeeper=self.cluster_config.zookeeper),
        )
        self.zk.start()
        return self

    def __exit__(self, type, value, traceback):
        self.zk.stop()

    def get_children(self, path, watch=None):
        """Returns the children of the specified node."""
        _log.debug(
            "ZK: Getting children of {path}".format(path=path),
        )
        return self.zk.get_children(path, watch)

    def get(self, path, watch=None):
        """Returns the data of the specified node."""
        _log.debug(
            "ZK: Getting {path}".format(path=path),
        )
        return self.zk.get(path, watch)

    def set(self, path, value):
        """Sets and returns new data for the specified node."""
        _log.debug(
            "ZK: Setting {path} to {value}".format(path=path, value=value)
        )
        return self.zk.set(path, value)

    def get_json(self, path, watch=None):
        """Reads the data of the specified node and converts it to json."""
        data, _ = self.get(path, watch)
        return load_json(data) if data else None

    def get_broker_metadata(self, broker_id):
        try:
            broker_json, _ = self.get(
                "/brokers/ids/{b_id}".format(b_id=broker_id)
            )
        except NoNodeError:
            _log.error(
                "broker '{b_id}' not found.".format(b_id=broker_id),
            )
            raise
        return load_json(broker_json)

    def get_brokers(self, names_only=False):
        """Get information on all the available brokers.

        :rtype : dict of brokers
        """
        try:
            broker_ids = self.get_children("/brokers/ids")
        except NoNodeError:
            _log.info(
                "cluster is empty."
            )
            return {}
        # Return broker-ids only
        if names_only:
            return {int(b_id): None for b_id in broker_ids}
        return {int(b_id): self.get_broker_metadata(b_id) for b_id in broker_ids}

    def get_topic_config(self, topic):
        """Get configuration information for specified topic.

        :rtype : dict of configuration
        """
        try:
            config_data = load_json(
                self.get(
                    "/config/topics/{topic}".format(topic=topic)
                )[0]
            )
        except NoNodeError as e:

            # Kafka version before 0.8.1 does not have "/config/topics/<topic_name>" path in ZK and
            # if the topic exists, return default dict instead of raising an Exception.
            # Ref: https://cwiki.apache.org/confluence/display/KAFKA/Kafka+data+structures+in+Zookeeper.

            topics = self.get_topics(topic_name=topic, fetch_partition_state=False)
            if len(topics) > 0:
                _log.info("Configuration not available for topic {topic}.".format(topic=topic))
                config_data = {"config": {}}
            else:
                _log.error(
                    "topic {topic} not found.".format(topic=topic)
                )
                raise e
        return config_data

    def set_topic_config(self, topic, value, kafka_version=(0, 10, )):
        """Set configuration information for specified topic.

        :topic : topic whose configuration needs to be changed
        :value :  config value with which the topic needs to be
            updated with. This would be of the form key=value.
            Example 'cleanup.policy=compact'
        :kafka_version :tuple kafka version the brokers are running on.
            Defaults to (0, 10, x). Kafka version 9 and kafka 10
            support this feature.
        """
        config_data = dump_json(value)

        try:
            # Change value
            return_value = self.set(
                "/config/topics/{topic}".format(topic=topic),
                config_data
            )
            # Create change
            version = kafka_version[1]

            # this feature is supported in kafka 9 and kafka 10
            assert version in (9, 10), "Feature supported with kafka 9 and kafka 10"

            if version == 9:
                # https://github.com/apache/kafka/blob/0.9.0.1/
                #     core/src/main/scala/kafka/admin/AdminUtils.scala#L334
                change_node = dump_json({
                    "version": 1,
                    "entity_type": "topics",
                    "entity_name": topic
                })
            else:  # kafka 10
                # https://github.com/apache/kafka/blob/0.10.2.1/
                #     core/src/main/scala/kafka/admin/AdminUtils.scala#L574
                change_node = dump_json({
                    "version": 2,
                    "entity_path": "topics/" + topic,
                })

            self.create(
                '/config/changes/config_change_',
                change_node,
                sequence=True
            )
        except NoNodeError as e:
            _log.error(
                "topic {topic} not found.".format(topic=topic)
            )
            raise e
        return return_value

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
            'ctime': <timestamp>,
            'partitions': {
                <p_id>:{
                    replicas: [<broker_id>, <broker_id>, ...],
                    isr: [<broker_id>, <broker_id>, ...],
                    controller_epoch: <val>,
                    leader_epoch: <val>,
                    version: 1,
                    leader: <broker-id>,
                    ctime: <timestamp>,
                }
            }
        }
        Note: By default we also fetch partition-state which results in
        accessing the zookeeper twice. If just partition-replica information is
        required fetch_partition_state should be set to False.
        """
        try:
            topic_ids = [topic_name] if topic_name else self.get_children(
                "/brokers/topics",
            )
        except NoNodeError:
            _log.error(
                "Cluster is empty."
            )
            return {}

        if names_only:
            return topic_ids
        topics_data = {}
        for topic_id in topic_ids:
            try:
                topic_info = self.get("/brokers/topics/{id}".format(id=topic_id))
                topic_data = load_json(topic_info[0])
                topic_ctime = topic_info[1].ctime / 1000.0
                topic_data['ctime'] = topic_ctime
            except NoNodeError:
                _log.info(
                    "topic '{topic}' not found.".format(topic=topic_id),
                )
                return {}
            # Prepare data for each partition
            partitions_data = {}
            for p_id, replicas in six.iteritems(topic_data['partitions']):
                partitions_data[p_id] = {}
                if fetch_partition_state:
                    # Fetch partition-state from zookeeper
                    partition_state = self._fetch_partition_state(topic_id, p_id)
                    partitions_data[p_id] = load_json(partition_state[0])
                    partitions_data[p_id]['ctime'] = partition_state[1].ctime / 1000.0
                else:
                    # Fetch partition-info from zookeeper
                    partition_info = self._fetch_partition_info(topic_id, p_id)
                    partitions_data[p_id]['ctime'] = partition_info.ctime / 1000.0
                partitions_data[p_id]['replicas'] = replicas
            topic_data['partitions'] = partitions_data
            topics_data[topic_id] = topic_data
        return topics_data

    def get_consumer_groups(self, consumer_group_id=None, names_only=False):
        """Get information on all the available consumer-groups.

        If names_only is False, only list of consumer-group ids are sent.
        If names_only is True, Consumer group offset details are returned
        for all consumer-groups or given consumer-group if given in dict
        format as:-

        {
            'group-id':
            {
                'topic':
                {
                    'partition': offset-value,
                    ...
                    ...
                }
            }
        }

        :rtype: dict of consumer-group offset details
        """
        if consumer_group_id is None:
            group_ids = self.get_children("/consumers")
        else:
            group_ids = [consumer_group_id]

        # Return consumer-group-ids only
        if names_only:
            return {g_id: None for g_id in group_ids}

        consumer_offsets = {}
        for g_id in group_ids:
            consumer_offsets[g_id] = self.get_group_offsets(g_id)
        return consumer_offsets

    def get_group_offsets(self, group, topic=None):
        """Fetch group offsets for given topic and partition otherwise all topics
        and partitions otherwise.


        {
            'topic':
            {
                'partition': offset-value,
                ...
                ...
            }
        }
        """
        group_offsets = {}
        try:
            all_topics = self.get_my_subscribed_topics(group)
        except NoNodeError:
            # No offset information of given consumer-group
            _log.warning(
                "No topics subscribed to consumer-group {group}.".format(
                    group=group,
                ),
            )
            return group_offsets
        if topic:
            if topic in all_topics:
                topics = [topic]
            else:
                _log.error(
                    "Topic {topic} not found in topic list {topics} for consumer"
                    "-group {consumer_group}.".format(
                        topic=topic,
                        topics=', '.join(topic for topic in all_topics),
                        consumer_group=group,
                    ),
                )
                return group_offsets
        else:
            topics = all_topics
        for topic in topics:
            group_offsets[topic] = {}
            try:
                partitions = self.get_my_subscribed_partitions(group, topic)
            except NoNodeError:
                _log.warning(
                    "No partition offsets found for topic {topic}. "
                    "Continuing to next one...".format(topic=topic),
                )
                continue
            # Fetch offsets for each partition
            for partition in partitions:
                path = "/consumers/{group_id}/offsets/{topic}/{partition}".format(
                    group_id=group,
                    topic=topic,
                    partition=partition,
                )
                try:
                    # Get current offset
                    offset_json, _ = self.get(path)
                    group_offsets[topic][partition] = load_json(offset_json)
                except NoNodeError:
                    _log.error("Path {path} not found".format(path=path))
                    raise
        return group_offsets

    def _fetch_partition_state(self, topic_id, partition_id):
        """Fetch partition-state for given topic-partition."""
        state_path = "/brokers/topics/{topic_id}/partitions/{p_id}/state"
        try:
            partition_state = self.get(
                state_path.format(topic_id=topic_id, p_id=partition_id),
            )
            return partition_state
        except NoNodeError:
            return {}  # The partition has no data

    def _fetch_partition_info(self, topic_id, partition_id):
        """Fetch partition info for given topic-partition."""
        info_path = "/brokers/topics/{topic_id}/partitions/{p_id}"
        try:
            _, partition_info = self.get(
                info_path.format(topic_id=topic_id, p_id=partition_id),
            )
            return partition_info
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

    def get_cluster_assignment(self):
        """Fetch the cluster layout in form of assignment from zookeeper"""
        plan = self.get_cluster_plan()
        assignment = {}
        for elem in plan['partitions']:
            assignment[
                (elem['topic'], elem['partition'])
            ] = elem['replicas']

        return assignment

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
        _log.debug("ZK: Creating node " + path)
        return self.zk.create(path, value, acl, ephemeral, sequence, makepath)

    def delete(self, path, recursive=False):
        """Deletes a Zookeeper node.

        :param: path: The zookeeper node path
        :param: recursive: Recursively delete node and all its children.
        """
        _log.debug("ZK: Deleting node " + path)
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
        self.delete(path, True)

    def delete_group(self, groupid):
        path = "/consumers/{groupid}".format(
            groupid=groupid,
        )
        self.delete(path, True)

    def execute_plan(self, plan, allow_rf_change=False):
        """Submit reassignment plan for execution."""
        reassignment_path = '{admin}/{reassignment_node}'\
            .format(admin=ADMIN_PATH, reassignment_node=REASSIGNMENT_NODE)
        plan_json = dump_json(plan)
        base_plan = self.get_cluster_plan()
        if not validate_plan(plan, base_plan, allow_rf_change=allow_rf_change):
            _log.error('Given plan is invalid. Aborting new reassignment plan ... {plan}'.format(plan=plan))
            return False
        # Send proposed-plan to zookeeper
        try:
            _log.info('Sending plan to Zookeeper...')
            self.create(reassignment_path, plan_json, makepath=True)
            _log.info(
                'Re-assign partitions node in Zookeeper updated successfully '
                'with {plan}'.format(plan=plan),
            )
            return True
        except NodeExistsError:
            _log.warning('Previous plan in progress. Exiting..')
            _log.warning('Aborting new reassignment plan... {plan}'.format(plan=plan))
            in_progress_plan = load_json(self.get(reassignment_path)[0])
            in_progress_partitions = [
                '{topic}-{p_id}'.format(
                    topic=p_data['topic'],
                    p_id=str(p_data['partition']),
                )
                for p_data in in_progress_plan['partitions']
            ]
            _log.warning(
                '{count} partition(s) reassignment currently in progress:-'
                .format(count=len(in_progress_partitions)),
            )
            _log.warning(
                '{partitions}. In Progress reassignment plan...'.format(
                    partitions=', '.join(in_progress_partitions),
                ),
            )
            return False
        except Exception as e:
            _log.error(
                'Could not re-assign partitions {plan}. Error: {e}'
                .format(plan=plan, e=e),
            )
            return False

    def get_cluster_plan(self):
        """Fetch cluster plan from zookeeper."""

        _log.info('Fetching current cluster-topology from Zookeeper...')
        cluster_layout = self.get_topics(fetch_partition_state=False)
        # Re-format cluster-layout
        partitions = [
            {
                'topic': topic_id,
                'partition': int(p_id),
                'replicas': partitions_data['replicas']
            }
            for topic_id, topic_info in six.iteritems(cluster_layout)
            for p_id, partitions_data in six.iteritems(topic_info['partitions'])
        ]
        return {
            'version': 1,
            'partitions': partitions
        }

    def get_pending_plan(self):
        """Read the currently running plan on reassign_partitions node."""
        reassignment_path = '{admin}/{reassignment_node}'\
            .format(admin=ADMIN_PATH, reassignment_node=REASSIGNMENT_NODE)
        try:
            result = self.get(reassignment_path)
            return load_json(result[0])
        except NoNodeError:
            return {}
