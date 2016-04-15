from behave import then
from behave import when
from util import call_cmd
from util import get_cluster_config

from yelp_kafka_tool.util.zookeeper import ZK


TEST_GROUP = 'test_group'
SET_OFFSET = 36


def offsets_data(topic, offset):
    return '''{topic}.{partition}={offset}'''.format(
        topic=topic,
        partition='0',
        offset=offset,
    )


def call_offset_set(offsets_data):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_set',
           TEST_GROUP,
           offsets_data]
    return call_cmd(cmd)


@when(u'we call the offset_set command with a groupid and offset data')
def step_impl2(context):
    context.offsets = offsets_data(context.topic, SET_OFFSET)
    call_offset_set(context.offsets)


@then(u'the committed offsets will match the specified offsets')
def step_impl3(context):
    cluster_config = get_cluster_config()
    with ZK(cluster_config) as zk:
        offsets = zk.get_group_offsets(TEST_GROUP)
    assert offsets[context.topic]["0"] == SET_OFFSET
