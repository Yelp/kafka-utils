from yelp_kafka_tool.kafka_cluster_manager.util import KafkaInterface
from yelp_kafka_tool.kafka_cluster_manager.cluster_info.util import (
    get_reduced_proposed_plan,
    confirm_execution,
    proposed_plan_json,
)


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
    log,
):
    result = "executed"
    # Get final-proposed-plan
    proposed_plan = get_reduced_proposed_plan(
        initial_assignment,
        curr_assignment,
        max_changes,
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
            log.info('Executing Proposed Plan')
            KafkaInterface().execute_plan(
                proposed_plan,
                zk.cluster_config.zookeeper,
                brokers,
                topics,
            )
        else:
            log.info('Proposed Plan won\'t be executed.')
        # Export proposed-plan to json file
        if proposed_plan_file:
            proposed_plan_json(proposed_plan, proposed_plan_file)
    else:
        log.info('No topic-partition layout changes proposed.')
    return result
