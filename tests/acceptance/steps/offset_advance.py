import os

from behave import then
from behave import when
from util import call_cmd
from util import get_cluster_config

from yelp_kafka_tool.util.zookeeper import ZK


def call_offset_advance(groupid):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_advance',
           groupid]
    return call_cmd(cmd)


@when(u'we call the offset_advance command with a groupid and topic')
def step_impl3(context):
    call_offset_advance(context.group)


@when(u'we call the offset_advance command and commit into kafka')
def step_impl3_2(context):
    if '0.9.0' == os.environ['KAFKA_VERSION']:
        call_offset_advance(context.group, context.topic, storage='kafka')


@then(u'the committed offsets will match the latest message offsets')
def step_impl4(context):
    cluster_config = get_cluster_config()
    with ZK(cluster_config) as zk:
        offsets = zk.get_group_offsets(context.group)
    assert offsets[context.topic]["0"] == context.msgs_produced
