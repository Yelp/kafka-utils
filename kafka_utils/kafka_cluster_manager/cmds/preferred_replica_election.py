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
import json
import logging
import sys

from .command import ClusterManagerCmd


class PreferredReplicaElectionCmd(ClusterManagerCmd):

    def __init__(self):
        super().__init__()
        self.log = logging.getLogger(self.__class__.__name__)

    def build_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            'preferred-replica-election',
            description='Generates json files for kafka-preferred-replica-election',
            help='This command is used to help generate kafka-preferred-replica-election'
                 ' json files based on input from kafka-check.'
        )
        subparser.add_argument(
            '--topic-partition-filter',
            type=str,
            required=True,
            help='Path to file containing a colon separated list of topic partitions to work on.'
                 ' This is useful if you only want to operate on a limited set of topic partitions'
                 ' Observe that kafka-check outputs colon separated lists, after ommitting the first $n'
                 ' lines e.g. kafka-check offline | tail -n+$N',
        )
        return subparser

    def run_command(self, cluster_topology, cluster_balancer):
        filter_set = self.get_topic_filter()
        output = {'partitions': []}
        # Validate the filter_set to check that all of the partitions provided are part of the cluster_topology
        for t_p in filter_set:
            if t_p not in cluster_topology.assignment:
                self.log.error(f'Topic partition {t_p} from filter not in cluster topology')
                sys.exit(1)
            topic, partition = t_p[0], t_p[1]
            output['partitions'].append({'topic': topic, 'partition': partition})

        self.log.info('Preferred replica assignment json generated. Run kafka-preferred-replica-election using this json:')
        print(json.dumps(output))

        if self.args.proposed_plan_file:
            self.write_json_plan(output, self.args.proposed_plan_file)
            self.log.info(f'Saved plan to file {self.args.proposed_plan_file}')
