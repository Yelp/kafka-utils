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
from __future__ import annotations

import argparse
from typing import Any
from typing import cast
from typing import Dict

from kafka_utils.kafka_check import status_code
from kafka_utils.kafka_check.status_code import prepare_terminate_message
from kafka_utils.kafka_check.status_code import terminate
from kafka_utils.util.config import ClusterConfig
from kafka_utils.util.zookeeper import ZK


class KafkaCheckCmd:
    """Interface used by all kafka_check commands
    The attributes cluster_config, args and zk are initialized on run().
    """

    def __init__(self):
        self.cluster_config = None
        self.args = None
        self.zk = None

    def build_subparser(self, subparsers: Any) -> Any:
        """Build the command subparser.

        :param subparsers: argpars subparsers
        :returns: subparser
        """
        raise NotImplementedError("Implement in subclass")

    def run_command(self) -> tuple[int, dict[str, Any]]:
        """Implement the command logic.
        When run_command is called cluster_config, args, and zk are already
        initialized.
        """
        raise NotImplementedError("Implement in subclass")

    def run(self, cluster_config: ClusterConfig, args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
        self.cluster_config = cluster_config
        self.args = args
        with ZK(self.cluster_config) as self.zk:
            broker_ids = get_broker_ids(self.zk)

            if args.controller_only and not is_controller(self.zk, args.broker_id):
                terminate(
                    status_code.OK,
                    cast(Dict[str, Any], prepare_terminate_message(
                        'Broker {} is not the controller, nothing to check'
                        .format(args.broker_id),
                    )),
                    args.json,
                )
            if args.first_broker_only and not broker_ids:
                terminate(
                    status_code.OK,
                    cast(Dict[str, Any], prepare_terminate_message(
                        'No brokers detected, nothing to check'
                    )),
                    args.json,
                )
            if args.first_broker_only and not is_first_broker(broker_ids, args.broker_id):
                terminate(
                    status_code.OK,
                    cast(Dict[str, Any], prepare_terminate_message(
                        'Broker {} has not the lowest id, nothing to check'
                        .format(args.broker_id),
                    )),
                    args.json,
                )
            return self.run_command()

    def add_subparser(self, subparsers: Any) -> None:
        self.build_subparser(subparsers).set_defaults(command=self.run)


def is_controller(zk: ZK, broker_id: int) -> bool:
    """Returns true if the specified broker_id is the controller of the cluster,
    false otherwise.
    """
    return broker_id == zk.get_json('/controller').get('brokerid')


def get_broker_ids(zk) -> list[int]:
    """Returns a list of integers for broker ids"""
    return list(zk.get_brokers())


def is_first_broker(broker_ids: list[int], broker_id: int) -> bool:
    """Returns true if broker_id is the lowest broker id in the cluster, false otherwise."""
    if not broker_ids:
        return False
    else:
        return broker_id == min(broker_ids)
