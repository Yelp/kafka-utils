from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import sys

from kafka import KafkaClient
from kazoo.exceptions import NoNodeError

from .offset_manager import OffsetManagerBase
from yelp_kafka_tool.kafka_consumer_manager.util import create_offsets
from yelp_kafka_tool.kafka_consumer_manager.util import fetch_offsets
from yelp_kafka_tool.kafka_consumer_manager.util import preprocess_topics
from yelp_kafka_tool.util.zookeeper import ZK


class CopyGroup(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_copy_group = subparsers.add_parser(
            "copy_group",
            description="Copy specified consumer group details to a new group.",
        )
        parser_copy_group.add_argument(
            'source_groupid',
            help="Consumer Group to be copied.",
        )
        parser_copy_group.add_argument(
            'dest_groupid',
            help="New name for the consumer group being copied to.",
        )
        parser_copy_group.add_argument(
            "--topic",
            help="Kafka topic whose offsets will be copied into destination group"
            " If no topic is specificed all topic offsets will be copied.",
        )
        parser_copy_group.add_argument(
            "--partitions",
            nargs='+',
            type=int,
            help="List of partitions within the topic. If no partitions are "
            "specified, offsets from all partitions of the topic shall "
            "be copied.",
        )
        parser_copy_group.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        if args.source_groupid == args.dest_groupid:
            print(
                "Error: Source group ID and destination group ID are same.",
                file=sys.stderr,
            )
            sys.exit(1)
        # Setup the Kafka client
        client = KafkaClient(cluster_config.broker_list)
        client.load_metadata_for_topics()
        source_topics = cls.preprocess_args(
            args.source_groupid,
            args.topic,
            args.partitions,
            cluster_config,
            client,
        )
        with ZK(cluster_config) as zk:
            try:
                topics_dest_group = zk.get_children(
                    "/consumers/{groupid}/offsets".format(
                        groupid=args.dest_groupid,
                    )
                )
            except NoNodeError:
                # Consumer Group ID doesn't exist.
                pass
            else:
                preprocess_topics(
                    args.source_groupid,
                    source_topics.keys(),
                    args.dest_groupid,
                    topics_dest_group,
                )

            # Fetch offsets
            source_offsets = fetch_offsets(zk, args.source_groupid, source_topics)
            create_offsets(zk, args.dest_groupid, source_offsets)
