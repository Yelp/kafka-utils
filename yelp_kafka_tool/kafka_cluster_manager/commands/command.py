import argparse
import logging


class ClusterManagerCmd(object):
    """Interface used by all kafka_cluster_manager commands"""

    log = logging.getLogger("ClusterManager")

    def add_subparser(self, subparser):
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
