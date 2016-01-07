from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import json
import sys
from collections import defaultdict

from kafka import KafkaClient
from kafka.common import KafkaUnavailableError
from yelp_kafka.monitoring import get_consumer_offsets_metadata

from .offset_manager import OffsetManagerBase


class OffsetSave(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_offset_save = subparsers.add_parser(
            "offset_save",
            description="Save current consumer offsets for the"
            " specified consumer group.",
            add_help=False,
        )
        parser_offset_save.add_argument(
            "-h",
            "--help",
            action="help",
            help="Show this help message and exit.",
        )
        parser_offset_save.add_argument(
            'groupid',
            help="Consumer Group ID whose offsets shall be fetched.",
        )
        parser_offset_save.add_argument(
            "--topic",
            help="Kafka topic whose offsets shall be fetched. If no topic is "
            "specified, offsets from all topics that the consumer is "
            "subscribed to, shall be fetched.",
        )
        parser_offset_save.add_argument(
            "--partitions",
            nargs='+',
            type=int,
            help="List of partitions within the topic. If no partitions are "
            "specified, offsets from all partitions of the topic shall "
            "be fetched.",
        )
        parser_offset_save.add_argument(
            "json_file",
            type=str,
            help="Export data in json format in the given file.",
        )
        parser_offset_save.set_defaults(command=cls.run)

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
        )
        try:
            consumer_offsets_metadata = get_consumer_offsets_metadata(
                client,
                args.groupid,
                topics_dict,
                False,
            )
        except KafkaUnavailableError:
            print(
                "Error: Encountered error with Kafka, please try again later.",
                file=sys.stderr,
            )
            raise

        # Warn the user if a topic being subscribed to does not exist in
        # Kafka.
        for topic in topics_dict:
            if topic not in consumer_offsets_metadata:
                print(
                    "Warning: Topic {topic} does not exist in Kafka"
                    .format(topic=topic),
                    file=sys.stderr,
                )

        # Build consumer-offset data in desired format
        current_consumer_offsets = defaultdict(dict)
        for topic, topic_offsets in consumer_offsets_metadata.iteritems():
            for partition_offset in topic_offsets:
                current_consumer_offsets[topic][partition_offset.partition] = partition_offset.current
        consumer_offsets_data = {args.groupid: current_consumer_offsets}

        # Save consumer-offsets to file
        with open(args.json_file, "w") as json_file:
            try:
                json.dump(consumer_offsets_data, json_file)
            except ValueError:
                print("Error: Invalid json data {data}".format(data=consumer_offsets_data))
                raise
            print("Consumer offset data saved in json-file {file}".format(file=args.json_file))
        client.close()
