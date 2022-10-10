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
import sys

from kafka.errors import UnknownMemberIdError

from .offset_manager import OffsetWriter
from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.offsets import get_current_consumer_offsets
from kafka_utils.util.offsets import nullify_offsets
from kafka_utils.util.offsets import set_consumer_offsets


class UnsubscribeTopics(OffsetWriter):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_unsubscribe_topics = subparsers.add_parser(
            "unsubscribe_topics",
            description="Delete topics and partitions by consumer group. This "
            "tool shall delete all offset metadata from Zookeeper.",
            add_help=False
        )
        parser_unsubscribe_topics.add_argument(
            "-h", "--help", action="help",
            help="Show this help message and exit."
        )
        parser_unsubscribe_topics.add_argument(
            'groupid',
            help="Consumer Group IDs whose metadata shall be deleted."
        )
        parser_unsubscribe_topics.add_argument(
            '--topic',
            help="Topic whose metadata shall be deleted. If either --topic or"
            "--topics are NOT specified, all topics that the consumer is"
            "subscribed to, shall be deleted."
        )
        parser_unsubscribe_topics.add_argument(
            '--partitions', nargs='+', type=int,
            help="List of partitions whose metadata shall be deleted. If no "
            "partitions are specified, all partitions within the topic shall "
            "be deleted. Only works with --topic, NOT with --topics."
        )
        parser_unsubscribe_topics.add_argument(
            '--topics', nargs='+',
            help="Topics whose metadata shall be deleted. If either --topic or"
            "--topics are NOT specified, all topics that the consumer is"
            "subscribed to, shall be deleted."
        )
        parser_unsubscribe_topics.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        # Setup the Kafka client
        client = KafkaToolClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        # if topic is not None topics_dict will contain only info about that
        # topic, otherwise it will contain info about all topics for the group
        topics_dict = cls.preprocess_args(
            args.groupid,
            args.topic,
            args.partitions,
            cluster_config,
            client,
            topics=args.topics,
            use_admin_client=args.use_admin_client,
        )

        topics = args.topics if args.topics else ([args.topic] if args.topic else [])

        unsubscriber = KafkaUnsubscriber(client)
        unsubscriber.unsubscribe_topics(
            args.groupid,
            topics,
            args.partitions,
            topics_dict,
        )


class TopicUnsubscriber:
    """Base class used to unsubscribe consumer groups from topic partitions."""

    def unsubscribe_topics(self, group, topics, partitions, topics_dict):
        # If a single topic and partitions are both specified,
        # then unsubscribe the group from the individual partitions

        # If only the topics are specified, the unsubscribe the group
        # from all the partitions of the topics.

        # If neither the topic nor partitions are specified, then
        # unsubscribe the group from all of the topics and partitions
        # that are found in the topics_dict that was preprocessed.
        if topics and len(topics) == 1 and partitions:
            self.unsubscribe_partitions(group, topics[0], partitions)
        elif topics:
            for topic in topics:
                try:
                    self.delete_topic(group, topic)
                except UnknownMemberIdError:
                    print(
                        "Error: Unable to unsubscribe group '{group_name}' from topic '{topic_name}'. \
                            Please ensure all consumers in this consumer group are stopped before \
                            trying to unsubscribe a consumer group with offsets stored in \
                            Kafka.".format(group_name=group, topic_name=topic),
                        file=sys.stderr,
                    )
                    raise

        else:
            for topic, partitions in topics_dict.items():
                self.delete_topic(group, topic)

    def unsubscribe_partitions(self, group, topic, partitions):
        raise NotImplementedError()

    def delete_topic(self, group, topic):
        raise NotImplementedError()


class KafkaUnsubscriber(TopicUnsubscriber):
    """Class used to unsubscribe consumer groups."""

    def __init__(self, client):
        self.client = client

    def unsubscribe_partitions(self, group, topic, partitions):
        offsets = {
            topic: {
                partition: 0
                for partition in partitions
            }
        }
        set_consumer_offsets(
            self.client,
            group,
            nullify_offsets(offsets),
        )

    def delete_topic(self, group, topic):
        offsets = get_current_consumer_offsets(
            self.client,
            group,
            [topic],
        )
        set_consumer_offsets(
            self.client,
            group,
            nullify_offsets(offsets),
        )
