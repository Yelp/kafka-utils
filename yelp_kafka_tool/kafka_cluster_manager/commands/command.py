import argparse
import logging
import sys

from ..cluster_info.util import confirm_execution
from yelp_kafka_tool.kafka_cluster_manager.util import KafkaInterface


class ClusterManagerCmd(object):
    """Interface used by all kafka_cluster_manager commands"""

    log = logging.getLogger("ClusterManager")

    def execute_plan(self, ct, zk, proposed_plan, to_apply, no_confirm):
        """Save proposed-plan and execute the same if requested."""
        # Execute proposed-plan
        if self.to_execute(to_apply, no_confirm):
            result = KafkaInterface().execute_plan(
                zk,
                proposed_plan,
                ct.brokers.values(),
                ct.topics.values(),
            )
            if not result:
                self.log.error('Plan execution unsuccessful. Exiting...')
                sys.exit(1)
            else:
                self.log.info('Plan sent to zookeeper for reassignment successfully.')
        else:
            self.log.info('Proposed plan won\'t be executed.')

    def to_execute(self, to_apply, no_confirm):
        """Confirm if proposed-plan should be executed."""
        if to_apply and (no_confirm or confirm_execution()):
            return True
        return False

    def add_subparser(self, subparsers):
        """Configure the subparser of the command

        :param subparser: argpars subparser
        """
        raise NotImplementedError("Implement in subclass")

    def positive_int(self, string):
        """Convert string to positive integer."""
        error_msg = 'Positive integer required, {string} given.'.format(string=string)
        try:
            value = int(string)
        except ValueError:
            raise argparse.ArgumentTypeError(error_msg)
        if value < 0:
            raise argparse.ArgumentTypeError(error_msg)
        return value

    def is_reassignment_pending(self, zk):
        """Return True if there are no reassignment tasks pending."""
        if zk.reassignment_in_progress():
            in_progress_plan = zk.get_in_progress_plan()
            if in_progress_plan:
                in_progress_partitions = in_progress_plan['partitions']
                self.log.info(
                    'Previous re-assignment in progress for {count} partitions.'
                    ' Current partitions in re-assignment queue: {partitions}'
                    .format(
                        count=len(in_progress_partitions),
                        partitions=in_progress_partitions,
                    )
                )
            else:
                self.log.warning(
                    'Previous re-assignment in progress. In progress partitions'
                    ' could not be fetched',
                )
            return False
        return True
