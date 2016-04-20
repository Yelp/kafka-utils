import json
import logging
from collections import OrderedDict

from .command import ClusterManagerCmd

DEFAULT_MAX_PARTITION_MOVEMENTS = 1
DEFAULT_MAX_LEADER_CHANGES = 5


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

    def generate_kafka_json(self, assignment_dict):
        """Assemble json string per examples in:
        http://kafka.apache.org/documentation.html#basic_ops_automigrate"""
        return json.dumps(
            OrderedDict((
                ('version', 1),
                (
                    'partitions',
                    [
                        {'topic': k[0], 'partition': k[1], 'replicas': v}
                        for k, v in assignment_dict.items()
                    ]
                ),
            )),
            sort_keys=True,
        )

    def run_command(self, ct):
        json_text = self.generate_kafka_json(ct.assignment)
        if self.args.json_out:
            with open(self.args.json_out, 'w') as f:
                self.log.info(
                    'writing assignments as json to: %s',
                    self.args.json_out)
                f.write(json_text)
        else:
            self.log.info('writing assignments as json to stdout')
            print json_text
