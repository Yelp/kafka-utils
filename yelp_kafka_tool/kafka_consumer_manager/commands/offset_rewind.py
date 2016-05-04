from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import sys

from kafka import KafkaClient

from .offset_manager import OffsetWriter
from yelp_kafka_tool.util.offsets import rewind_consumer_offsets


class OffsetRewind(OffsetWriter):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_offset_rewind = subparsers.add_parser(
            "offset_rewind",
            description="Rewind consumer offsets for the specified consumer "
            "group to the earliest message in the topic partition",
            add_help=False
        )
        parser_offset_rewind.add_argument(
            "-h", "--help", action="help",
            help="Show this help message and exit."
        )
        parser_offset_rewind.add_argument(
            'groupid',
            help="Consumer Group ID whose consumer offsets shall be rewinded."
        )
        parser_offset_rewind.add_argument(
            "--topic",
            help="Kafka topic whose offsets shall be manipulated. If no topic is "
            "specified, offsets from all topics that the consumer is "
            "subscribed to, shall be rewinded."
        )
        parser_offset_rewind.add_argument(
            "--partitions", nargs='+', type=int,
            help="List of partitions within the topic. If no partitions are "
            "specified, offsets from all partitions of the topic shall "
            "be rewinded."
        )
        parser_offset_rewind.add_argument(
            '--storage', choices=['zookeeper', 'kafka'],
            help="String describing where to store the committed offsets."
        )
        parser_offset_rewind.add_argument(
            '--force',
            help="Force the offset of the group to be committed even if "
            "it does not already exist."
        )
        parser_offset_rewind.set_defaults(command=OffsetRewind.run)

    @classmethod
    def run(cls, args, cluster_config):
        # Setup the Kafka client
        client = KafkaClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        topics_dict = cls.preprocess_args(
            args.groupid,
            args.topic,
            args.partitions,
            cluster_config,
            client,
            force=args.force,
        )
        try:
            rewind_consumer_offsets(
                client,
                args.groupid,
                topics_dict,
                args.storage,
            )
        except TypeError:
            print(
                "Error: Badly formatted input, please re-run command "
                "with --help option.", file=sys.stderr
            )
            raise

        client.close()
