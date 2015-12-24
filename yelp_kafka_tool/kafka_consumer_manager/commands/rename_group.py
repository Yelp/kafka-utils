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


class RenameGroup(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_rename_group = subparsers.add_parser(
            "rename_group",
            description="Rename specified consumer group ID to a new name. "
            "This tool shall migrate all offset metadata in Zookeeper.",
            add_help=False
        )
        parser_rename_group.add_argument(
            "-h", "--help", action="help",
            help="Show this help message and exit."
        )
        parser_rename_group.add_argument(
            'old_groupid',
            help="Consumer Group ID to be renamed."
        )
        parser_rename_group.add_argument(
            'new_groupid',
            help="New name for the consumer group ID."
        )
        parser_rename_group.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        if args.old_groupid == args.new_groupid:
            print(
                "Error: Old group ID and new group ID are the same.",
                file=sys.stderr,
            )
            sys.exit(1)
        # Setup the Kafka client
        client = KafkaClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        topics_dict = cls.preprocess_args(
            args.old_groupid, None, None, cluster_config, client
        )
        with ZK(cluster_config) as zk:
            try:
                topics = zk.get_children(
                    "/consumers/{groupid}/offsets".format(
                        groupid=args.new_groupid
                    )
                )
            except NoNodeError:
                # Consumer Group ID doesn't exist.
                pass
            else:
                preprocess_topics(
                    cls,
                    args.old_groupid,
                    topics_dict.keys(),
                    args.new_groupid,
                    topics,
                )

            old_offsets = fetch_offsets(zk, args.old_groupid, topics_dict)
            create_offsets(zk, args.new_groupid, old_offsets)
            try:
                old_base_path = "/consumers/{groupid}".format(
                    groupid=args.old_groupid,
                )
                zk.delete(old_base_path, recursive=True)
            except:
                print(
                    "Error: Unable to migrate all metadata in Zookeeper. "
                    "Please re-run the command.",
                    file=sys.stderr
                )
                raise
