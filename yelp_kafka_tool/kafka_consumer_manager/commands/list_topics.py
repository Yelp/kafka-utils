from __future__ import absolute_import

import sys

from kafka import KafkaClient

from .offset_manager import OffsetManagerBase


class ListTopics(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_list_topics = subparsers.add_parser(
            "list_topics",
            description="List topics by consumer group.",
            add_help=False
        )
        parser_list_topics.add_argument(
            "-h", "--help", action="help",
            help="Show this help message and exit."
        )
        parser_list_topics.add_argument(
            'groupid',
            help="Consumer Group ID whose topics shall be fetched."
        )
        parser_list_topics.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        # Setup the Kafka client
        client = KafkaClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        topics_dict = cls.preprocess_args(
            args.groupid, None, None,
            cluster_config, client,
            False
        )
        if not topics_dict:
            print(
                "Consumer Group ID: {group} does not exist in "
                "Zookeeper".format(
                    group=args.groupid
                )
            )
            sys.exit(1)

        print("Consumer Group ID: {groupid}".format(groupid=args.groupid))
        for topic, partitions in topics_dict.iteritems():
            print("\tTopic: {topic}".format(topic=topic))
            print("\t\tPartitions: {partitions}".format(partitions=partitions))
