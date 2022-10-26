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
from __future__ import annotations

import logging
import re
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Sequence

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from kazoo.exceptions import NoNodeError
from kazoo.protocol.states import WatchedEvent
from kazoo.protocol.states import ZnodeStat
from kazoo.retry import KazooRetry
from kazoo.security import ACL
from typing_extensions import TypedDict

from kafka_utils.util.config import ClusterConfig
from kafka_utils.util.serialization import dump_json
from kafka_utils.util.serialization import load_json
from kafka_utils.util.validation import validate_plan

ADMIN_PATH = "/admin"
REASSIGNMENT_NODE = "reassign_partitions"
_log = logging.getLogger('kafka-zookeeper-manager')


class TopicDataPartitionDict(TypedDict, total=False):
    replicas: list[int]
    isr: list[int]
    controller_epcoh: int
    leader_epoch: int
    version: int
    leader: int
    ctime: float


class TopicDataDict(TypedDict):
    version: int
    partitions: dict[str, TopicDataPartitionDict]


class ClusterPlanPartitionDict(TypedDict):
    topic: str
    partition: int
    replicas: list[int]


class ClusterPlanDict(TypedDict):
    version: int
    partitions: list[ClusterPlanPartitionDict]


class ZK:
    """Opens a connection to a kafka zookeeper. "
    "To be used in the 'with' statement."""

    def __init__(self, cluster_config: ClusterConfig) -> None:
        self.cluster_config = cluster_config

    def __enter__(self) -> ZK:
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

    def __exit__(self, type: type | None, value: BaseException, traceback: TracebackType) -> None:
        self.zk.stop()

    def get_children(self, path: str, watch: Callable[[WatchedEvent], None] | None = None) -> list[str]:
        """Returns the children of the specified node."""
        _log.debug(
            f"ZK: Getting children of {path}",
        )
        return self.zk.get_children(path, watch)

    def get(self, path: str, watch: Callable[[WatchedEvent], None] | None = None) -> tuple[bytes, ZnodeStat]:
        """Returns the data of the specified node."""
        _log.debug(
            f"ZK: Getting {path}",
        )
        return self.zk.get(path, watch)

    def set(self, path: str, value: bytes) -> ZnodeStat:
        """Sets and returns new data for the specified node."""
        _log.debug(
            f"ZK: Setting {path} to {value!r}"
        )
        return self.zk.set(path, value)

    def get_json(self, path: str, watch: Callable[[WatchedEvent], None] | None = None) -> Any:
        """Reads the data of the specified node and converts it to json."""
        data, _ = self.get(path, watch)
        return load_json(data) if data else None

    def get_broker_metadata(self, broker_id: str) -> dict[str, Any]:
        try:
            broker_json = load_json(self.get(
                f"/brokers/ids/{broker_id}"
            )[0])
            if (broker_json['host'] is None):
                pattern = '(?:[SSL|INTERNAL|PLAINTEXTSASL].*://)?(?P<host>[^:/ ]+).?(?P<port>[0-9]*).*'
                result = re.search(pattern, broker_json['endpoints'][0])
                assert result is not None
                broker_json['host'] = result.group('host')
        except NoNodeError:
            _log.error(
                f"broker '{broker_id}' not found.",
            )
            raise
        return broker_json

    def get_brokers(self, names_only: bool = False) -> dict[int, dict[str, Any] | None]:
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

    def get_topic_config(self, topic: str) -> dict[str, Any]:
        """Get configuration information for specified topic.

        :rtype : dict of configuration
        """
        return self._get_entity_config(
            "topics",
            topic,
            lambda topic: len(self.get_topics(topic_name=topic, fetch_partition_state=False)) > 0,
        )

    def set_topic_config(self, topic: str, value: str, kafka_version: tuple[int, int] = (0, 10, )) -> None:
        """Set configuration information for specified topic.

        :topic : topic whose configuration needs to be changed
        :value :  config value with which the topic needs to be
            updated with. This would be of the form key=value.
            Example 'cleanup.policy=compact'
        :kafka_version :tuple kafka version the brokers are running on.
            Defaults to (0, 10, x). Kafka version 9 and kafka 10
            support this feature.
        """
        self._set_entity_config("topics", topic, value, kafka_version)

    def get_broker_config(self, broker_id: str) -> dict[str, Any]:
        """Get configuration information for specified broker.

        :rtype : dict of configuration
        """
        return self._get_entity_config(
            "brokers",
            broker_id,
            lambda broker_id: broker_id in self.get_brokers(names_only=True)
        )

    def set_broker_config(self, broker_id: str, value: str, kafka_version: tuple[int, int] = (0, 10, )) -> ZnodeStat:
        """Set configuration information for specified broker.

        :broker_id : broker whose configuration needs to be changed
        :value :  config value with which the topic needs to be
            updated with. This would be of the form key=value.
            Example 'cleanup.policy=compact'
        :kafka_version :tuple kafka version the brokers are running on.
            Defaults to (0, 10, x). Versions above Kafka 0.9 support this feature.
        """
        return self._set_entity_config("brokers", broker_id, value, kafka_version)

    def _get_entity_config(self, entity_type: str, entity_name: str, entity_exists: Callable[[str], bool]) -> dict[str, Any]:
        """Get configuration information for specified broker.

        :entity_type : "brokers" or "topics"
        :entity_name : broker id or topic name
        :entity_exists : fn(entity_name) -> bool to determine whether an entity
                            exists. used to determine whether to throw an exception
                            when a configuration cannot be found for the given entity_name
        :rtype : dict of configuration
        """
        assert entity_type in ("brokers", "topics"), "Supported entities are brokers and topics"

        try:
            config_data = load_json(
                self.get(
                    f"/config/{entity_type}/{entity_name}"
                )[0]
            )
        except NoNodeError as e:
            if entity_exists(entity_name):
                _log.info("Configuration not available for {entity_type} {entity_name}.".format(
                    entity_type=entity_type,
                    entity_name=entity_name,
                ))
                config_data = {"config": {}}
            else:
                _log.error(f"{entity_type} {entity_name} not found")
                raise e

        return config_data

    def _set_entity_config(self, entity_type: str, entity_name: str, value: str, kafka_version: tuple[int, int] = (0, 10, )) -> ZnodeStat:
        """Set configuration information for specified entity.

        :entity_type : "brokers" or "topics"
        :entity_name : broker id or topic name
        :value :  config value with which the entity needs to be
            updated with. This would be of the form key=value.
            Example 'cleanup.policy=compact'
        :kafka_version :tuple kafka version the brokers are running on.
            Defaults to (0, 10, x). Versions above Kafka 0.9 support this feature.
        """
        assert entity_type in ("brokers", "topics"), "Supported entities are brokers and topics"

        config_data = dump_json(value)

        try:
            # Change value
            return_value = self.set(
                f"/config/{entity_type}/{entity_name}",
                config_data,
            )

            # Create change
            assert kafka_version >= (0, 9, ), "Feature supported with kafka 0.9 and above"

            if kafka_version < (0, 10, ):
                # https://github.com/apache/kafka/blob/0.9.0.1/
                #     core/src/main/scala/kafka/admin/AdminUtils.scala#L334
                change_node = dump_json({
                    "version": 1,
                    "entity_type": entity_type,
                    "entity_name": entity_name,
                })
            else:  # kafka 0.10+
                # https://github.com/apache/kafka/blob/0.10.2.1/
                #     core/src/main/scala/kafka/admin/AdminUtils.scala#L574
                change_node = dump_json({
                    "version": 2,
                    "entity_path": "{entity_type}/{entity_name}".format(
                        entity_type=entity_type,
                        entity_name=entity_name
                    )
                })

            self.create(
                '/config/changes/config_change_',
                change_node,
                sequence=True
            )
        except NoNodeError as e:
            _log.error(
                f"{entity_type}: {entity_name} not found."
            )
            raise e
        return return_value

    def get_topics(
        self,
        topic_name: str | None = None,
        names_only: bool = False,
        fetch_partition_state: bool = True,
    ) -> dict[str, TopicDataDict] | list[str]:
        topic_names = [topic_name] if topic_name else None
        return self.get_multiple_topics(topic_names, names_only, fetch_partition_state)

    def get_multiple_topics(
        self,
        topic_names: list[str] | None = None,
        names_only: bool = False,
        fetch_partition_state: bool = True,
    ) -> dict[str, TopicDataDict] | list[str]:
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
            if not topic_names:
                topic_names = self.get_children("/brokers/topics")
        except NoNodeError:
            _log.error(
                "Cluster is empty."
            )
            return {}

        if names_only:
            return topic_names
        topics_data = {}
        for topic_id in topic_names:
            try:
                topic_info = self.get(f"/brokers/topics/{topic_id}")
                topic_data = load_json(topic_info[0])
                topic_ctime = topic_info[1].ctime / 1000.0
                topic_data['ctime'] = topic_ctime
            except NoNodeError:
                _log.info(
                    f"topic '{topic_id}' not found.",
                )
                return {}
            # Prepare data for each partition
            partitions_data: dict[str, TopicDataPartitionDict] = {}
            for p_id, replicas in topic_data['partitions'].items():
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

    def get_consumer_groups(self, consumer_group_id: str | None = None, names_only: bool = False) -> dict[str, dict[str, dict[str, Any]] | None]:
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

        consumer_offsets: dict[str, dict[str, dict[str, Any]] | None] = {}
        for g_id in group_ids:
            consumer_offsets[g_id] = self.get_group_offsets(g_id)
        return consumer_offsets

    def get_group_offsets(self, group: str, topic: str | None = None) -> dict[str, dict[str, Any]]:
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
        group_offsets: dict[str, dict[str, Any]] = {}
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
                    _log.error(f"Path {path} not found")
                    raise
        return group_offsets

    def _fetch_partition_state(self, topic_id: str, partition_id: str) -> Any:
        """Fetch partition-state for given topic-partition."""
        state_path = "/brokers/topics/{topic_id}/partitions/{p_id}/state"
        try:
            partition_state = self.get(
                state_path.format(topic_id=topic_id, p_id=partition_id),
            )
            return partition_state
        except NoNodeError:
            return {}  # The partition has no data

    def _fetch_partition_info(self, topic_id: str, partition_id: str) -> Any:
        """Fetch partition info for given topic-partition."""
        info_path = "/brokers/topics/{topic_id}/partitions/{p_id}"
        try:
            _, partition_info = self.get(
                info_path.format(topic_id=topic_id, p_id=partition_id),
            )
            return partition_info
        except NoNodeError:
            return {}  # The partition has no data

    def get_my_subscribed_topics(self, groupid: str) -> list[str]:
        """Get the list of topics that a consumer is subscribed to

        :param: groupid: The consumer group ID for the consumer
        :returns list of kafka topics
        :rtype: list
        """
        path = f"/consumers/{groupid}/offsets"
        return self.get_children(path)

    def get_my_subscribed_partitions(self, groupid: str, topic: str) -> list[str]:
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

    def get_cluster_assignment(self, topic_names: list[str] | None = None) -> dict[tuple[str, int], list[int]]:
        """Fetch the cluster layout in form of assignment from zookeeper"""
        plan = self.get_cluster_plan(topic_names)
        assignment = {}
        for elem in plan['partitions']:
            assignment[
                (elem['topic'], elem['partition'])
            ] = elem['replicas']

        return assignment

    def create(
        self,
        path: str,
        value: bytes = b'',
        acl: Sequence[ACL] | None = None,
        ephemeral: bool = False,
        sequence: bool = False,
        makepath: bool = False,
    ) -> str:
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

    def delete(self, path: str, recursive: bool = False) -> bool:
        """Deletes a Zookeeper node.

        :param: path: The zookeeper node path
        :param: recursive: Recursively delete node and all its children.
        """
        _log.debug("ZK: Deleting node " + path)
        return self.zk.delete(path, recursive=recursive)

    def delete_topic(self, groupid: str, topic: str) -> None:
        path = "/consumers/{groupid}/offsets/{topic}".format(
            groupid=groupid,
            topic=topic,
        )
        self.delete(path, True)

    def delete_group(self, groupid: str) -> None:
        path = "/consumers/{groupid}".format(
            groupid=groupid,
        )
        self.delete(path, True)

    def execute_plan(self, plan: ClusterPlanDict, allow_rf_change: bool = False, allow_rf_mismatch: bool = False) -> bool:
        """Submit reassignment plan for execution."""
        reassignment_path = '{admin}/{reassignment_node}'\
            .format(admin=ADMIN_PATH, reassignment_node=REASSIGNMENT_NODE)
        plan_json = dump_json(plan)
        topic_names_from_proposed_plan = set()
        for partition in plan['partitions']:
            topic_names_from_proposed_plan.add(partition['topic'])
        base_plan = self.get_cluster_plan(topic_names=list(topic_names_from_proposed_plan))
        if not validate_plan(plan, base_plan, allow_rf_change=allow_rf_change, allow_rf_mismatch=allow_rf_mismatch):
            _log.error(f'Given plan is invalid. Aborting new reassignment plan ... {plan}')
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
            _log.warning(f'Aborting new reassignment plan... {plan}')
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

    def get_cluster_plan(self, topic_names: list[str] | None = None) -> ClusterPlanDict:
        """Fetch cluster plan from zookeeper."""

        _log.info('Fetching current cluster-topology from Zookeeper...')
        cluster_layout = self.get_multiple_topics(topic_names, fetch_partition_state=False)
        assert isinstance(cluster_layout, dict)
        # Re-format cluster-layout
        partitions: list[ClusterPlanPartitionDict] = [
            {
                'topic': topic_id,
                'partition': int(p_id),
                'replicas': partitions_data['replicas']
            }
            for topic_id, topic_info in cluster_layout.items()
            for p_id, partitions_data in topic_info['partitions'].items()
        ]
        return {
            'version': 1,
            'partitions': partitions
        }

    def get_pending_plan(self) -> Any:
        """Read the currently running plan on reassign_partitions node."""
        reassignment_path = '{admin}/{reassignment_node}'\
            .format(admin=ADMIN_PATH, reassignment_node=REASSIGNMENT_NODE)
        try:
            result = self.get(reassignment_path)
            return load_json(result[0])
        except NoNodeError:
            return {}
