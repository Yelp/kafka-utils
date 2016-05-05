from __future__ import absolute_import
from __future__ import print_function

from kafka import KafkaClient

from .offset_manager import OffsetWriter
from yelp_kafka_tool.util.offsets import delete_group


class DeleteGroup(OffsetWriter):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_delete_group = subparsers.add_parser(
            "delete_group",
            description="Delete a consumer group by groupid. This "
            "tool shall delete all group offset metadata from Zookeeper.",
            add_help=False
        )
        parser_delete_group.add_argument(
            "-h", "--help", action="help",
            help="Show this help message and exit."
        )
        parser_delete_group.add_argument(
            'groupid',
            help="Consumer Group IDs whose metadata shall be deleted."
        )
        parser_delete_group.add_argument(
            '--storage', choices=['zookeeper', 'kafka'],
            help="String describing where to store the committed offsets.",
        )
        parser_delete_group.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        # Setup the Kafka client
        client = KafkaClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        delete_group(cluster_config, args.groupid, offset_storage=args.storage)
