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
def execute_plan(
    initial_assignment,
    curr_assignment,
    max_changes,
    apply,
    no_confirm,
    proposed_plan_file,
    zk,
    brokers,
    topics,
):
    # Get final-proposed-plan
    result = get_reduced_proposed_plan(
        initial_assignment,
        curr_assignment,
        max_changes,
    )
    # Valid plan found, Execute or display the plan
    if result:
        print('in result')
        # Display plan only if user-confirmation is required
        log_only = False if apply and not no_confirm or not apply else True
        display_assignment_changes(result[1], result[2], result[3], log_only)

        to_execute = False
        proposed_plan = result[0]
        if apply:
            if no_confirm:
                to_execute = True
            else:
                if confirm_execution():
                    to_execute = True
        # Execute proposed-plan
        if to_execute:
            _log.info('Executing Proposed Plan')
            KafkaInterface().execute_plan(
                proposed_plan,
                zk.cluster_config.zookeeper,
                brokers,
                topics,
            )
        else:
            _log.info('Proposed Plan won\'t be executed.')
        # Export proposed-plan to json file
        if proposed_plan_file:
            proposed_plan_json(proposed_plan, proposed_plan_file)
    else:
        # No new-plan
        _log.info('No topic-partition layout changes proposed.')
    return
