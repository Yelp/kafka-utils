from behave import then
from behave import when

from .util import call_cmd
from .util import get_cluster_config
from yelp_kafka_tool.util.zookeeper import ZK


def call_unsubscribe_topics(groupid):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'unsubscribe_topics',
           groupid]
    return call_cmd(cmd)


@when('we call the unsubscribe_topics command')
def step_impl2(context):
    call_unsubscribe_topics(context.group)


@then(u'the committed offsets will no longer exist in zookeeper')
def step_impl4(context):
    cluster_config = get_cluster_config()
    with ZK(cluster_config) as zk:
        offsets = zk.get_group_offsets(context.group)
    assert context.topic not in offsets
