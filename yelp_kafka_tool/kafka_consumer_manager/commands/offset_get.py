from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import sys

from kafka import KafkaClient
from yelp_kafka.monitoring import get_consumer_offsets_metadata

from .offset_manager import OffsetManagerBase
from yelp_kafka_tool.util import print_json


class OffsetGet(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_offset_get = subparsers.add_parser(
            "offset_get",
            description="Get consumer offsets for the"
            " specified consumer group",
            add_help=False
        )
        parser_offset_get.add_argument(
            "-h", "--help", action="help",
            help="Show this help message and exit."
        )
        parser_offset_get.add_argument(
            'groupid',
            help="Consumer Group ID whose offsets shall be fetched."
        )
        parser_offset_get.add_argument(
            "--topic",
            help="Kafka topic whose offsets shall be fetched. If no topic is "
            "specified, offsets from all topics that the consumer is "
            "subscribed to, shall be fetched."
        )
        parser_offset_get.add_argument(
            "--partitions", nargs='+', type=int,
            help="List of partitions within the topic. If no partitions are "
            "specified, offsets from all partitions of the topic shall "
            "be fetched."
        )
        parser_offset_get.add_argument(
            "--watermark",
            choices=["high", "low", "current", "all"],
            help="Type of offset watermark. \"high\" represents the offset "
            "corresponding to the latest message. \"low\" represents "
            "the offset corresponding to the earliest message. \"current\" "
            "represents the current commited offset for this consumer",
            default="all"
        )
        parser_offset_get.add_argument(
            "-j", "--json", action="store_true",
            help="Export data in json format."
        )
        parser_offset_get.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        # Setup the Kafka client
        client = KafkaClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        topics_dict = cls.preprocess_args(
            args.groupid, args.topic, args.partitions, cluster_config, client
        )
        try:
            consumer_offsets_metadata = get_consumer_offsets_metadata(
                client, args.groupid, topics_dict, False
            )
        except:
            print(
                "Error: Encountered error with Kafka, please try again later.",
                file=sys.stderr
            )
            raise

        # Warn the user if a topic being subscribed to does not exist in
        # Kafka.
        for topic in topics_dict:
            if topic not in consumer_offsets_metadata:
                print(
                    "Warning: Topic {topic} or one or more of it's partitions "
                    "do not exist in Kafka".format(
                        topic=topic
                    ), file=sys.stderr
                )

        client.close()
        if args.json:
            print_json(consumer_offsets_metadata)
        else:
            cls.print_output(consumer_offsets_metadata, args.watermark)

    @classmethod
    def print_output(cls, consumer_offsets_metadata, watermark_filter):
        for topic, metadata_tuples in consumer_offsets_metadata.iteritems():
            print ("Topic Name: {topic}".format(topic=topic))
            for metadata_tuple in metadata_tuples:
                print (
                    "\tPartition ID: {partition}".format(
                        partition=metadata_tuple.partition
                    )
                )
                if watermark_filter == "all" or watermark_filter == "high":
                    print(
                        "\t\tHigh Watermark: {high}".format(
                            high=metadata_tuple.highmark
                        )
                    )
                if watermark_filter == "all" or watermark_filter == "low":
                    print(
                        "\t\tLow Watermark: {low}".format(
                            low=metadata_tuple.lowmark
                        )
                    )
                if watermark_filter == "all" or watermark_filter == "current":
                    print(
                        "\t\tCurrent Offset: {current}".format(
                            current=metadata_tuple.current
                        )
                    )
