import os

from behave import then
from behave import when
from util import get_cluster_config

from yelp_kafka_tool.util.client import KafkaToolClient
from yelp_kafka_tool.util.monitoring import _get_current_offsets
from yelp_kafka_tool.util.offsets import set_consumer_offsets


TEST_KAFKA_COMMIT_GROUP = 'kafka_commit_group'
TEST_OFFSET = 56


def commit_offsets(offsets, group):
    # Setup the Kafka client
    config = get_cluster_config()
    client = KafkaToolClient(config.broker_list)
    set_consumer_offsets(
        client,
        group,
        offsets,
        offset_storage='kafka',
    )
    client.close()


def fetch_offsets(group, topics):
    # Setup the Kafka client
    config = get_cluster_config()
    client = KafkaToolClient(config.broker_list)
    offsets = _get_current_offsets(client, group, topics, False, 'dual')
    client.close()
    return offsets


@when(u'we commit some offsets for a group into kafka')
def step_impl4(context):
    if '0.9.0' == os.environ['KAFKA_VERSION']:
        context.offsets = {context.topic: {0: TEST_OFFSET}}
        commit_offsets(context.offsets, TEST_KAFKA_COMMIT_GROUP)


@when(u'we fetch offsets for the group with the dual option')
def step_impl4_2(context):
    if '0.9.0' == os.environ['KAFKA_VERSION']:
        topics = context.offsets.keys()
        context.fetched_offsets = fetch_offsets(
            TEST_KAFKA_COMMIT_GROUP,
            topics,
        )


@then(u'the fetched offsets will match the committed offsets')
def step_impl5(context):
    if '0.9.0' == os.environ['KAFKA_VERSION']:
        assert context.fetched_offsets == context.offsets
