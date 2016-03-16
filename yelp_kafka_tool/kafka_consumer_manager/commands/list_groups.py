from __future__ import absolute_import
from __future__ import print_function

import sys

from kazoo.exceptions import NoNodeError

from .offset_manager import OffsetManagerBase
from yelp_kafka_tool.util.zookeeper import ZK


class ListGroups(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_list_groups = subparsers.add_parser(
            "list_groups",
            description="List consumer groups.",
            add_help=False,
        )
        parser_list_groups.add_argument(
            "-h", "--help", action="help",
            help="Show this help message and exit."
        )
        parser_list_groups.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        with ZK(cluster_config) as zk:
            try:
                groupids = zk.get_children("/consumers")
            except NoNodeError:
                print(
                    "Error: No consumers node found in zookeeper",
                    file=sys.stderr,
                )
            else:
                print("Consumer Groups:")
                for groupid in groupids:
                    print("\t{groupid}".format(groupid=groupid))
                print(
                    "{num_groups} groups found for cluster {cluster_name} "
                    "of type {cluster_type}".format(
                        num_groups=len(groupids),
                        cluster_name=cluster_config.name,
                        cluster_type=cluster_config.type,
                    ),
                )
