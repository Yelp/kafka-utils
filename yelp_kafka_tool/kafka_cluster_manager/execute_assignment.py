import logging

from .cluster_info.display import display_assignment_changes
from .cluster_info.util import (
    get_reduced_proposed_plan,
    confirm_execution,
    proposed_plan_json,
)
from .util import KafkaInterface

_log = logging.getLogger('kafka-cluster-manager')


# Execute or display cluster-topology in zookeeper
def evaluate_assignment(ct, zk, max_changes, to_apply, no_confirm, plan_file, script_path):
    # Get final-proposed-plan
    result = get_reduced_proposed_plan(
        ct.initial_assignment,
        ct.assignment,
        max_changes,
    )
    log_only = True if no_confirm else False
    if result:
        # Display or store plan
        display_assignment_changes(result[1], result[2], result[3], log_only)
        # Store and/or execute plan
        execute = to_execute(to_apply, no_confirm)
        save_execute_plan(ct, zk, result[0], execute, plan_file, script_path)
    else:
        # No new-plan
        msgStr = 'No topic-partition layout changes proposed.'
        if log_only:
            _log.info(msgStr)
        else:
            print(msgStr)
    return


def save_execute_plan(ct, zk, proposed_plan, execute, plan_file, script_path):
    """Save proposed-plan and execute the same if requested."""
    # Export proposed-plan to json file
    if plan_file:
        proposed_plan_json(proposed_plan, plan_file)
    # Execute proposed-plan
    if execute:
        _log.info('Executing Proposed Plan')
        kafka = KafkaInterface(script_path)
        kafka.execute_plan(
            zk,
            proposed_plan,
            ct.brokers.values(),
            ct.topics.values(),
        )
    else:
        _log.info('Proposed Plan won\'t be executed.')


def to_execute(to_apply, no_confirm):
    """Confirm if proposed-plan should be executed."""
    if to_apply:
        if no_confirm:
            return True
        else:
            if confirm_execution():
                return True
    return False
