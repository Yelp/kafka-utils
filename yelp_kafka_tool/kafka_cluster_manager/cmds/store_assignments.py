import json
import logging

from .command import ClusterManagerCmd
from yelp_kafka_tool.kafka_cluster_manager.util import assignment_to_plan


class StoreAssignmentsCmd(ClusterManagerCmd):

    def __init__(self):
        super(StoreAssignmentsCmd, self).__init__()
        self.log = logging.getLogger(self.__class__.__name__)

    def build_subparser(self, subparsers):
        subparser = subparsers.add_parser(
            'store_assignments',
            description='Emit json encoding the current assignment of '
                        'partitions to replicas.',
            help='''This command will not mutate the cluster\'s state.
                 Output json is of this form:
                 {"version":1,"partitions":[
                    {"topic": "foo1", "partition": 2, "replicas": [1, 2]},
                    {"topic": "foo1", "partition": 0, "replicas": [3, 4]},
                    {"topic": "foo2", "partition": 2, "replicas": [1, 2]},
                    {"topic": "foo2", "partition": 0, "replicas": [3, 4]},
                    {"topic": "foo1", "partition": 1, "replicas": [2, 3]},
                    {"topic": "foo2", "partition": 1, "replicas": [2, 3]}]}'''
        )
        subparser.add_argument(
            '--json_out',
            type=str,
            help=('Path to json output file. '
                  'Will output to stdout if not set. '
                  'If file exists already, it will be clobbered.')
        )
        return subparser

    def run_command(self, ct):
        plan_json = json.dumps(assignment_to_plan(ct.assignment))
        if self.args.json_out:
            with open(self.args.json_out, 'w') as f:
                self.log.info(
                    'writing assignments as json to: %s',
                    self.args.json_out,
                )
                f.write(plan_json)
        else:
            self.log.info('writing assignments as json to stdout')
            print plan_json
