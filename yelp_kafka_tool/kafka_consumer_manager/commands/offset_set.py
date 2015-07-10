from __future__ import (
    absolute_import,
    print_function,
    unicode_literals,
)

import sys
from collections import defaultdict

from kafka import KafkaClient
from yelp_kafka.offsets import set_consumer_offsets

from .offset_manager import OffsetWriter


class OffsetSet(OffsetWriter):
    new_offsets_dict = defaultdict(dict)

    @classmethod
    def topics_dict(cls, string):
        try:
            topic, partition_offset = string.rsplit(".", 1)
            partition, offset = partition_offset.split("=", 1)
            cls.new_offsets_dict[topic][int(partition)] = int(offset)
        except ValueError:
            print(
                "Error: Badly formatted input, please re-run command "
                "with --help option.", file=sys.stderr
            )
            sys.exit(1)

    @classmethod
    def add_parser(cls, subparsers):
        parser_offset_set = subparsers.add_parser(
            "offset_set",
            description="Modify consumer offsets for the specified consumer "
            "group to the specified offset.",
            add_help=False
        )
        parser_offset_set.add_argument(
            "-h", "--help", action="help",
            help="Show this help message and exit."
        )
        parser_offset_set.add_argument(
            'groupid',
            help="Consumer Group ID whose consumer offsets shall be modified."
        )

        parser_offset_set.add_argument(
            "newoffsets", nargs='+', metavar=('<topic>.<partition>=<offset>'),
            type=cls.topics_dict,
            help="Tuple containing the Kafka topic, partition and "
            "the the intended "
            "new offset."
        )

        parser_offset_set.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        # Setup the Kafka client
        client = KafkaClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        # Let's verify that the consumer does exist in Zookeeper
        cls.get_topics_from_consumer_group_id(
            cluster_config,
            args.groupid
        )

        try:
            results = set_consumer_offsets(
                client,
                args.groupid,
                cls.new_offsets_dict
            )
        except TypeError:
            print(
                "Error: Badly formatted input, please re-run command "
                "with --help option.", file=sys.stderr
            )
            raise

        client.close()

        if results:
            final_error_str = ("Error: Unable to commit consumer offsets for:\n")
            for result in results:
                error_str = (
                    "  Topic: {topic} Partition: {partition} Error: {error}\n".format(
                        topic=result.topic,
                        partition=result.partition,
                        error=result.error
                    )
                )
                final_error_str += error_str
            print(final_error_str, file=sys.stderr)
            sys.exit(1)
