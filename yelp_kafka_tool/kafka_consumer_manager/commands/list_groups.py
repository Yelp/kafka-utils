from __future__ import absolute_import
from __future__ import print_function

import logging
import sys
from collections import defaultdict

from kafka.common import ConsumerTimeout
from kafka.common import FailedPayloadsError
from kafka.common import KafkaUnavailableError
from kafka.common import LeaderNotAvailableError
from kafka.common import NotLeaderForPartitionError
from kafka.util import read_short_string
from kafka.util import relative_unpack
from kazoo.exceptions import NoNodeError
from yelp_kafka.config import KafkaConsumerConfig
from yelp_kafka.consumer_group import KafkaConsumerGroup
from yelp_kafka.error import PartitionerError

from .offset_manager import OffsetManagerBase
from yelp_kafka_tool.util.zookeeper import ZK


CONSUMER_OFFSET_TOPIC = '__consumer_offsets'


class ListGroups(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_list_groups = subparsers.add_parser(
            "list_groups",
            description="List consumer groups.",
            add_help=False,
        )
        parser_list_groups.set_defaults(command=cls.run)

    @classmethod
    def get_zookeeper_groups(cls, cluster_config):
        '''Get the group_id of groups committed into Zookeeper.'''
        with ZK(cluster_config) as zk:
            try:
                return zk.get_children("/consumers")
            except NoNodeError:
                print(
                    "Error: No consumers node found in zookeeper",
                    file=sys.stderr,
                )

    @classmethod
    def get_kafka_groups(cls, cluster_config):
        '''Get the group_id of groups committed into Kafka.'''
        kafka_group_reader = KafkaGroupReader(cluster_config)
        try:
            kafka_group_reader.read_groups()
            return kafka_group_reader.groups.keys()
        except PartitionerError:
            print(
                "Error: No consumer offsets topic found in Kafka",
                file=sys.stderr,
            )

    @classmethod
    def print_groups(cls, groups, cluster_config):
        print("Consumer Groups:")
        for groupid in groups:
            print("\t{groupid}".format(groupid=groupid))
        print(
            "{num_groups} groups found for cluster {cluster_name} "
            "of type {cluster_type}".format(
                num_groups=len(groups),
                cluster_name=cluster_config.name,
                cluster_type=cluster_config.type,
            ),
        )

    @classmethod
    def run(cls, args, cluster_config):
        zk_groups = cls.get_zookeeper_groups(cluster_config)
        kafka_groups = cls.get_kafka_groups(cluster_config)
        groups = set()
        if zk_groups:
            groups.update(zk_groups)
        if kafka_groups:
            groups.update(kafka_groups)
        cls.print_groups(groups, cluster_config)


class InvalidMessageException(Exception):
    pass


class KafkaGroupReader:

    def __init__(self, kafka_config):
        self.log = logging.getLogger(__name__)
        self.kafka_config = kafka_config
        self.config = KafkaConsumerConfig(
            'offset_monitoring_consumer',
            self.kafka_config,
            auto_offset_reset='smallest',
            auto_commit=False,
            partitioner_cooldown=1,
            consumer_timeout_ms=10000,
        )
        self.kafka_groups = defaultdict(set)

    def read_groups(self):
        self.log.info("Kafka consumer running")
        self.consumer = KafkaConsumerGroup(
            [CONSUMER_OFFSET_TOPIC],
            self.config,
        )
        with self.consumer:
            self.log.info("Consumer ready")
            while True:
                try:
                    message = self.consumer.next()
                except ConsumerTimeout:
                    break
                except (
                    FailedPayloadsError,
                    KafkaUnavailableError,
                    LeaderNotAvailableError,
                    NotLeaderForPartitionError,
                ) as e:
                    self.log.warning("Got %s, retrying", e.__class__.__name__)
                self.process_consumer_offset_message(message)

    def parse_consumer_offset_message(self, message):
        key = bytearray(message.key)
        ((key_schema,), cur) = relative_unpack('>h', key, 0)
        if key_schema not in [0, 1]:
            raise InvalidMessageException()   # This is not an offset commit message
        (group, cur) = read_short_string(key, cur)
        (topic, cur) = read_short_string(key, cur)
        ((partition,), cur) = relative_unpack('>l', key, cur)
        if message.value:
            value = bytearray(message.value)
            ((value_schema,), cur) = relative_unpack('>h', value, 0)
            if value_schema not in [0, 1]:
                raise InvalidMessageException()  # Unrecognized message value
            ((offset,), cur) = relative_unpack('>q', value, cur)
        else:
            offset = None  # Offset was deleted
        return str(group), str(topic), partition, offset

    def process_consumer_offset_message(self, message):
        try:
            group, topic, partition, offset = self.parse_consumer_offset_message(message)
        except InvalidMessageException:
            return

        if offset:
            self.kafka_groups[group].add(topic)
        else:  # No offset means group deletion
            self.kafka_groups.pop(group, None)

    @property
    def groups(self):
        """Return a dictionary of group and topics in the format:
            {'group_name': ['topic1', 'topic2', ...] ...}
        """
        return self.kafka_groups
