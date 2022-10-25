# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse
import sys
from typing import Any

from kafka.errors import UnknownMemberIdError

from .offset_manager import OffsetWriter
from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.config import ClusterConfig
from kafka_utils.util.offsets import advance_consumer_offsets


class OffsetAdvance(OffsetWriter):

    @classmethod
    def setup_subparser(cls, subparsers: Any) -> None:
        parser_offset_advance = subparsers.add_parser(
            "offset_advance",
            description="Advance consumer offsets for the specified consumer "
            "group to the latest message in the topic partition",
            add_help=False
        )
        parser_offset_advance.add_argument(
            "-h", "--help", action="help",
            help="Show this help message and exit."
        )
        parser_offset_advance.add_argument(
            'groupid',
            help="Consumer Group ID whose consumer offsets shall be advanced."
        )
        parser_offset_advance.add_argument(
            "--topic",
            help="Kafka topic whose offsets shall be manipulated. If no topic "
            "is specified, offsets from all topics that the consumer is "
            "subscribed to, shall be advanced."
        )
        parser_offset_advance.add_argument(
            "--partitions", nargs='+', type=int,
            help="List of partitions within the topic. If no partitions are "
            "specified, offsets from all partitions of the topic shall "
            "be advanced."
        )
        parser_offset_advance.add_argument(
            '--force',
            action='store_true',
            help="Force the offset of the group to be committed even if "
            "it does not already exist."
        )
        parser_offset_advance.set_defaults(command=cls.run)

    @classmethod
    def run(cls, args: argparse.Namespace, cluster_config: ClusterConfig) -> None:
        # Setup the Kafka client
        client = KafkaToolClient(cluster_config.broker_list)
        client.load_metadata_for_topics()

        topics_dict = cls.preprocess_args(
            args.groupid,
            args.topic,
            args.partitions,
            cluster_config,
            client,
            force=args.force,
            use_admin_client=args.use_admin_client,
        )
        try:
            advance_consumer_offsets(
                client,
                args.groupid,
                topics_dict,
            )
        except TypeError:
            print(
                "Error: Badly formatted input, please re-run command ",
                "with --help option.", file=sys.stderr
            )
            raise
        except UnknownMemberIdError:
            print(
                "Unable to advance offsets for group '{group_name}' from topic '{topic_name}'. \
                    You must ensure none of the consumers with this consumer group id are running before \
                    trying to advance the offsets stored in Kafka for this consumer group. Try stopping all \
                    of your consumers.".format(group_name=args.groupid, topic_name=args.topic),
            )
            raise

        client.close()
