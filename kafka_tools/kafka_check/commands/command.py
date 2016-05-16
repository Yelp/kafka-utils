# -*- coding: utf-8 -*-
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
from kafka_tools.kafka_check import status_code
from kafka_tools.kafka_check.status_code import terminate
from kafka_tools.util.zookeeper import ZK


class KafkaCheckCmd(object):
    """Interface used by all kafka_check commands
    The attributes cluster_config, args and zk are initialized on run().
    """

    def __init__(self):
        self.cluster_config = None
        self.args = None
        self.zk = None

    def build_subparser(self, subparsers):
        """Build the command subparser.

        :param subparsers: argpars subparsers
        :returns: subparser
        """
        raise NotImplementedError("Implement in subclass")

    def run_command(self):
        """Implement the command logic.
        When run_command is called cluster_config, args, and zk are already
        initialized.
        """
        raise NotImplementedError("Implement in subclass")

    def run(self, cluster_config, args):
        self.cluster_config = cluster_config
        self.args = args
        with ZK(self.cluster_config) as self.zk:
            if args.controller_only:
                check_run_on_controller(self.zk, self.args)
            return self.run_command()

    def add_subparser(self, subparsers):
        self.build_subparser(subparsers).set_defaults(command=self.run)


def get_controller_id(zk):
    return zk.get_json('/controller').get('brokerid')


def parse_meta_properties_file(content):
    for line in content:
        parts = line.rstrip().split("=")
        if len(parts) == 2 and parts[0] == "broker.id":
            return int(parts[1])
    return None


def read_generated_broker_id(meta_properties_path):
    """reads broker_id from meta.properties file.

    :param string meta_properties_path: path for meta.properties file
    :returns int: broker_id from meta_properties_path
    """
    try:
        with open(meta_properties_path, 'r') as f:
            broker_id = parse_meta_properties_file(f)
    except IOError:
        terminate(
            status_code.WARNING,
            "Cannot open meta.properties file: {path}".format(
                path=meta_properties_path,
            ),
        )
    except ValueError:
        terminate(status_code.WARNING, "Broker id not valid")

    if broker_id is None:
        terminate(status_code.WARNING, "Autogenerated broker id missing from data directory")

    return broker_id


def get_broker_id(data_path):
    """This function will look into the data folder to get the automatically created
    broker_id.

    :param string data_path: the path to the kafka data folder
    :returns int: the real broker_id
    """

    # Path to the meta.properties file. This is used to read the automatic broker id
    # if the given broker id is -1
    META_FILE_PATH = "{data_path}/meta.properties"

    if not data_path:
        terminate(status_code.WARNING, "You need to specify the data_path if broker_id == -1")
    meta_properties_path = META_FILE_PATH.format(data_path=data_path)
    return read_generated_broker_id(meta_properties_path)


def check_run_on_controller(zk, args):
    """Kafka 0.9 supports automatic broker ids. If args.broker_id is set to -1,
    it will call get_broker_id() for parse broker_id from meta.properties file.
    """
    if args.broker_id is None:
        terminate(status_code.WARNING, "Broker id is not specified")

    if args.broker_id != -1:
        broker_id = args.broker_id
    else:
        broker_id = get_broker_id(args.data_path)

    controller_id = get_controller_id(zk)

    # This check is only executed by the controller
    if broker_id != controller_id:
        terminate(status_code.OK, 'Broker %s is not the controller, nothing to check' % (broker_id))
