from __future__ import absolute_import

from kafka import KafkaClient
from kazoo.exceptions import NoNodeError

from .offset_manager import OffsetWriter
from yelp_kafka_tool.util.zookeeper import ZK


class DeleteTopics(OffsetWriter):
    @classmethod
    def setup_subparser(cls, subparsers):
        parser_delete_topics = subparsers.add_parser(
            "delete_topics",
            description="Delete topics and partitions by consumer group. This "
            "tool shall delete all offset metadata from Zookeeper.",
            add_help=False
        )
        parser_delete_topics.add_argument(
            "-h", "--help", action="help",
            help="Show this help message and exit."
        )
        parser_delete_topics.add_argument(
            'groupid',
            help="Consumer Group IDs whose metadata shall be deleted."
        )
        parser_delete_topics.add_argument(
            '--topic',
            help="Topic whose metadata shall be deleted. If no topic is "
            "specified, all topics that the consumer is subscribed to, shall "
            "be deleted."
        )
        parser_delete_topics.add_argument(
            '--partitions', nargs='+', type=int,
            help="List of partitions whose metadata shall be deleted. If no "
            "partitions are specified, all partitions within the topic shall "
            "be deleted."
        )
        parser_delete_topics.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args, cluster_config):
        # Setup the Kafka client
        client = KafkaClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        topics_dict = cls.preprocess_args(
            args.groupid, args.topic, args.partitions, cluster_config, client
        )
        with ZK(cluster_config) as zk:
            for topic, partitions in topics_dict.iteritems():
                try:
                    zk.delete_topic_partitions(
                        args.groupid,
                        topic,
                        partitions
                    )
                except NoNodeError:
                    # If node doesn't exit, nothing to do.
                    pass
                if not zk.get_my_subscribed_partitions(args.groupid, topic):
                    zk.delete_topic(args.groupid, topic)
