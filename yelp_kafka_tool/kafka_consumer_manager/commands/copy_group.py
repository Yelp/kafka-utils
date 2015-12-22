from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import sys
from collections import defaultdict

from kafka import KafkaClient
from kazoo.exceptions import NoNodeError

from .offset_manager import OffsetManagerBase
from yelp_kafka_tool.util.zookeeper import ZK


class CopyGroup(OffsetManagerBase):

    @classmethod
    def setup_subparser(cls, subparsers):
        parser_copy_group = subparsers.add_parser(
            "copy_group",
            description="Copy specified consumer group details to a new group.",
            add_help=False,
        )
        parser_copy_group.add_argument(
            "-h",
            "--help",
            action="help",
            help="Show this help message and exit.",
        )
        parser_copy_group.add_argument(
            'source_groupid',
            help="Consumer Group to be copied.",
        )
        parser_copy_group.add_argument(
            'dest_groupid',
            help="New name for the consumer group being copied to..",
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
            None,
            None,
            cluster_config,
            client,
        )
        with ZK(cluster_config) as zk:
            try:
                dest_topics = zk.get_children(
                    "/consumers/{groupid}/offsets".format(
                        groupid=args.dest_groupid,
                    )
                )
            except NoNodeError:
                # Consumer Group ID doesn't exist.
                pass
            else:
                # Is the new consumer already subscribed to any of these topics?
                for topic in dest_topics:
                    if topic in source_topics:
                        print(
                            "Error: Consumer Group ID: {groupid} is already "
                            "subscribed to topic: {topic}.\nPlease delete this "
                            "topic from either group before re-running the "
                            "command.".format(
                                groupid=args.dest_groupid,
                                topic=topic,
                            ),
                            file=sys.stderr,
                        )
                        sys.exit(1)
                # Let's confirm what the user intends to do.
                if dest_topics:
                    in_str = (
                        "Consumer Group: {dest_groupid} already exists.\nTopics "
                        "subscribed to by the consumer groups are listed "
                        "below:\n{source_groupid}: {source_group_topics}\n"
                        "{dest_groupid}: {dest_group_topics}\nDo you intend to copy into"
                        "existing consumer destination-group? (y/n)".format(
                            source_groupid=args.source_groupid,
                            source_group_topics=source_topics.keys(),
                            dest_groupid=args.dest_groupid,
                            dest_group_topics=dest_topics,
                        )
                    )
                    cls.prompt_user_input(in_str)

            source_offsets = defaultdict(dict)
            for topic, partitions in source_topics.iteritems():
                for partition in partitions:
                    node_info = zk.get(
                        "/consumers/{groupid}/offsets/{topic}/{partition}".format(
                            groupid=args.source_groupid,
                            topic=topic,
                            partition=partition
                        )
                    )
                    offset, _ = node_info
                    source_offsets[topic][partition] = offset
            # Create new offsets
            for topic, partition_offsets in source_offsets.iteritems():
                for partition, offset in partition_offsets.iteritems():
                    new_path = "/consumers/{groupid}/offsets/{topic}/{partition}".format(
                        groupid=args.dest_groupid,
                        topic=topic,
                        partition=partition,
                    )
                    try:
                        zk.create(new_path, value=bytes(offset), makepath=True)
                    except:
                        print(
                            "Error: Unable to migrate all metadata in Zookeeper."
                            " Please re-run the command.",
                            file=sys.stderr,
                        )
                        raise
