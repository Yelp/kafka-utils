import argparse
import json
import logging
import sys
from collections import defaultdict

from yelp_kafka_tool.kafka_cluster_manager.util import KafkaInterface


class ClusterManagerCmd(object):
    """Interface used by all kafka_cluster_manager commands"""

    log = logging.getLogger("ClusterManager")

    def execute_plan(self, zk, plan, to_apply, no_confirm):
        """Save proposed-plan and execute the same if requested."""
        # Execute proposed-plan
        if self.should_execute(to_apply, no_confirm):
            result = KafkaInterface().execute_plan(zk, plan)
            if not result:
                self.log.error('Plan execution unsuccessful. Exiting...')
                sys.exit(1)
            else:
                self.log.info('Plan sent to zookeeper for reassignment successfully.')
        else:
            self.log.info('Proposed plan won\'t be executed.')

    def should_execute(self, to_apply, no_confirm):
        """Confirm if proposed-plan should be executed."""
        return to_apply and (no_confirm or self.confirm_execution())

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
        in_progress_plan = zk.get_pending_plan()
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
            return True
        else:
            return False

    def get_reduced_assignment(
        self,
        original_assignment,
        new_assignment,
        max_partition_movements,
        max_leader_only_changes,
    ):
        """Reduce the assignment based on the total actions.

        Actions represent actual partition movements
        and/or changes in preferred leader.
        Get the difference of original and proposed assignment
        and take the subset of this plan for given limit.

        Argument(s):
        original_assignment:    Current assignment of cluster in zookeeper
        new_assignment:         New proposed-assignment of cluster
        max_partition_movements:Maximum number of partition-movements in
                                final set of actions
        max_leader_only_changes:Maximum number of actions with leader only changes
        :return:
        :reduced_assignment:    Final reduced assignment
        :tot_actions:           Total actions to be executed
        :type:                  integer
        """
        if (not original_assignment or not new_assignment or
                max_partition_movements < 0 or max_leader_only_changes < 0):
            return {}

        # The replica set stays the same for leaders only changes
        leaders_changes = [
            (t_p, new_assignment[t_p])
            for t_p, replica in original_assignment.iteritems()
            if replica != new_assignment[t_p] and
            set(replica) == set(new_assignment[t_p])
        ]

        # The replica set is different for partitions changes
        # Here we create a list of tuple ((topic, partion), # replica movements)
        partitions_changes = [
            (
                t_p,
                len(set(replica) - set(new_assignment[t_p])),
            )
            for t_p, replica in original_assignment.iteritems()
            if set(replica) != set(new_assignment[t_p])
        ]

        self.log.info(
            "Total number of actions before reduction: %s.",
            len(partitions_changes) + len(leaders_changes)
        )
        # Extract reduced plan maximizing uniqueness of topics
        reduced_actions = self._extract_actions_unique_topics(
            partitions_changes,
            max_partition_movements,
        )
        reduced_partitions_changes = [
            (t_p, new_assignment[t_p]) for t_p in reduced_actions
        ]
        self.log.info(
            "Number of partition changes: %s."
            " Number of leader changes: %s",
            len(reduced_partitions_changes),
            min(max_leader_only_changes, len(leaders_changes)),
        )
        # Merge leaders and partition changes and generate the assignment
        reduced_assignment = {
            t_p: replicas
            for t_p, replicas in (
                reduced_partitions_changes + leaders_changes[:max_leader_only_changes]
            )
        }
        return reduced_assignment

    def _extract_actions_unique_topics(self, movement_counts, max_movements):
        """Extract actions limiting to given max value such that
           the resultant has the minimum possible number of duplicate topics.

           Algorithm:
           1. Group actions by by topic-name: {topic: action-list}
           2. Iterate through the dictionary in circular fashion and keep
              extracting actions with until max_partition_movements
              are reached.
           :param movement_counts: list of tuple ((topic, partition), movement count)
           :param max_movements: max number of movements to extract
           :return: list of tuple (topic, partitions) to include in the reduced plan
        """
        # Group actions by topic
        topic_actions = defaultdict(list)
        for t_p, replica_change_cnt in movement_counts:
            topic_actions[t_p[0]].append((t_p, replica_change_cnt))

        # Create reduced assignment minimizing duplication of topics
        extracted_actions = []
        curr_partition_cnt = 0
        action_available = True
        while curr_partition_cnt < max_movements and action_available:
            action_available = False
            for topic, actions in topic_actions.iteritems():
                for action in actions:
                    if curr_partition_cnt + action[1] > max_movements:
                        # Remove action since it won't be possible to use it
                        actions.remove(action)
                    else:
                        # Append (topic, partition) to the list of movements
                        action_available = True
                        extracted_actions.append(action[0])
                        curr_partition_cnt += action[1]
                        actions.remove(action)
                        break
        return extracted_actions

    def confirm_execution(self):
        """Confirm from your if proposed-plan be executed."""
        permit = ''
        while permit.lower() not in ('yes', 'no'):
            permit = raw_input('Execute Proposed Plan? [yes/no] ')
        if permit.lower() == 'yes':
            return True
        else:
            return False

    def write_json_plan(self, proposed_layout, proposed_plan_file):
        """Dump proposed json plan to given output file for future usage."""
        with open(proposed_plan_file, 'w') as output:
            json.dump(proposed_layout, output)
