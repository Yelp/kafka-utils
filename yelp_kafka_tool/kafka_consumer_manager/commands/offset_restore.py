from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import json
import sys
from collections import defaultdict

from kafka import KafkaClient
from yelp_kafka.monitoring import get_consumer_offsets_metadata
from yelp_kafka.offsets import set_consumer_offsets

from .offset_manager import OffsetManagerBase


class OffsetRestore(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_offset_restore = subparsers.add_parser(
            "offset_restore",
            description="Commit current consumer offsets for consumer group"
            " specified in given json file.",
            add_help=False,
        )
        parser_offset_restore.add_argument(
            "-h",
            "--help",
            action="help",
            help="Show this help message and exit.",
        )
        parser_offset_restore.add_argument(
            "--json-file",
            required=True,
            type=str,
            help="Json file containing offset information",
        )
        parser_offset_restore.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        # Setup the Kafka client
        client = KafkaClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        with open(args.json_file, 'r') as consumer_offsets_json:
            try:
                consumer_offsets_data = json.load(consumer_offsets_json)
                consumer_group = consumer_offsets_data.keys()[0]
                topics_offset_data = consumer_offsets_data.values()[0]
                topic_partitions = dict(
                    (topic, [int(partition) for partition in offset_data.keys()])
                    for topic, offset_data in topics_offset_data.iteritems()
                )
                # Get offset data from kafka-client
                consumer_offsets_metadata = get_consumer_offsets_metadata(
                    client,
                    consumer_group,
                    topic_partitions,
                    False,
                )
                new_offsets_dict = defaultdict(dict)
                for topic, partitions in topic_partitions.iteritems():
                    cls.validate_topic_partitions(
                        client,
                        topic,
                        partitions,
                        consumer_offsets_metadata,
                    )
                    # Validate current offsets in range of low and highmarks
                    # Currently we only validate for positive offsets and warn
                    # if out of range of low and highmarks
                    for curr_partition_offsets in consumer_offsets_metadata[topic]:
                        partition = curr_partition_offsets.partition
                        if partition not in topic_partitions[topic]:
                            continue
                        lowmark = curr_partition_offsets.lowmark
                        highmark = curr_partition_offsets.highmark
                        new_offset = topics_offset_data[topic][str(partition)]
                        if new_offset < 0:
                            print(
                                "Error: Given offset: {offset} is negative"
                                .format(offset=new_offset),
                                file=sys.stderr,
                            )
                            sys.exit(1)
                        if new_offset < lowmark or new_offset > highmark:
                            print(
                                "Warning: Given offset {offset} for topic-partition "
                                "{topic}:{partition} is outside the range of lowmark "
                                "{lowmark} and highmark {highmark}".format(
                                    offset=new_offset,
                                    topic=topic,
                                    partition=partition,
                                    lowmark=lowmark,
                                    highmark=highmark,
                                )
                            )
                        new_offsets_dict[topic][int(partition)] = int(new_offset)

                    # Commit offsets
                    set_consumer_offsets(client, consumer_group, new_offsets_dict)
            except ValueError:
                print(
                    "Error: Given consumer-data json data-file {file} could not be "
                    "parsed".format(file=args.json_file),
                    file=sys.stderr,
                )
                raise
            except IndexError:
                print(
                    "Error: Given consumer-offset data file {file} could not parsed"
                    .format(file=args.json_file),
                    file=sys.stderr,
                )
                raise
        client.close()

    @classmethod
    def validate_topic_partitions(cls, client, topic, partitions, consumer_offsets_metadata):
        # Validate topics
        if topic not in consumer_offsets_metadata:
            print(
                "Error: Topic {topic} do not exist in Kafka"
                .format(topic=topic),
                file=sys.stderr,
            )
            sys.exit(1)

        # Validate partition-list
        complete_partitions_list = client.get_partition_ids_for_topic(topic)
        if not set(partitions).issubset(complete_partitions_list):
            print(
                "Error: Some partitions amongst {partitions} are not "
                "part of complete partition list {complete_list} for "
                "topic: {topic}.".format(
                    partitions=', '.join(str(p) for p in partitions),
                    complete_list=', '.join(str(p) for p in complete_partitions_list),
                    topic=topic,
                ),
                file=sys.stderr,
            )
            sys.exit(1)
