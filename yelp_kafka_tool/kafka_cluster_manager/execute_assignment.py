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
    result = "executed"
    # Get final-proposed-plan

    proposed_plan, red_curr_plan_list, red_proposed_plan_list, total_actions = \
        get_reduced_proposed_plan(
            initial_assignment,
            curr_assignment,
            max_changes,
        )

    log_only = False if apply and not no_confirm or not apply else True
    display_assignment_changes(
        red_curr_plan_list,
        red_proposed_plan_list,
        total_actions,
        log_only,
    )
    # Execute or display the plan
    if proposed_plan:
        to_execute = False
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
        _log.info('No topic-partition layout changes proposed.')
    return result
