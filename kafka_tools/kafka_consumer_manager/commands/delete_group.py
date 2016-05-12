from __future__ import absolute_import
from __future__ import print_function

import sys

from kafka import KafkaClient

from .offset_manager import OffsetWriter
from kafka_tools.util.offsets import get_current_consumer_offsets
from kafka_tools.util.offsets import nullify_offsets
from kafka_tools.util.offsets import set_consumer_offsets
from kafka_tools.util.zookeeper import ZK


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

        if not args.storage or args.storage == 'zookeeper':
            cls.delete_group_zk(cluster_config, args.groupid)
        elif args.storage == 'kafka':
            cls.delete_group_kafka(client, args.groupid)
        else:
            print(
                "Error: Invalid offset storage option: "
                "{}.".format(args.storage),
                file=sys.stderr,
            )
            sys.exit(1)

    @classmethod
    def delete_group_zk(cls, cluster_config, group):
        with ZK(cluster_config) as zk:
            zk.delete_group(group)

    @classmethod
    def delete_group_kafka(cls, client, group):
        offsets = get_current_consumer_offsets(
            client,
            group,
            offset_storage='kafka',
        )
        set_consumer_offsets(
            client,
            group,
            nullify_offsets(offsets),
            offset_storage='kafka',
        )
