from behave import then
from behave import when

from .util import get_cluster_config
from kafka_tools.util.client import KafkaToolClient
from kafka_tools.util.monitoring import get_current_offsets
from kafka_tools.util.offsets import set_consumer_offsets


TEST_KAFKA_COMMIT_GROUP = 'kafka_commit_group'
TEST_OFFSET = 56


def commit_offsets(offsets, group, storage):
    # Setup the Kafka client
    config = get_cluster_config()
    client = KafkaToolClient(config.broker_list)
    set_consumer_offsets(
        client,
        group,
        offsets,
        offset_storage=storage,
    )
    client.close()


def fetch_offsets(group, topics, storage):
    # Setup the Kafka client
    config = get_cluster_config()
    client = KafkaToolClient(config.broker_list)
    offsets = get_current_offsets(client, group, topics, False, storage)
    client.close()
    return offsets


@when(u'we commit some offsets for a group into kafka')
def step_impl4(context):
    context.offsets = {context.topic: {0: TEST_OFFSET}}
    context.group = TEST_KAFKA_COMMIT_GROUP
    commit_offsets(context.offsets, context.group, 'kafka')


@when(u'we fetch offsets for the group with the dual option')
def step_impl4_2(context):
    topics = context.offsets.keys()
    context.fetched_offsets = fetch_offsets(
        TEST_KAFKA_COMMIT_GROUP,
        topics,
        'dual'
    )


@when(u'we fetch offsets for the group with the kafka option')
def step_impl4_3(context):
    topics = context.offsets.keys()
    context.fetched_offsets = fetch_offsets(
        TEST_KAFKA_COMMIT_GROUP,
        topics,
        'kafka'
    )


@then(u'the fetched offsets will match the committed offsets')
def step_impl5(context):
    assert context.fetched_offsets == context.offsets
