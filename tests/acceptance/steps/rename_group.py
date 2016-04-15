from behave import then
from behave import when
from util import call_cmd
from util import get_cluster_config

from yelp_kafka_tool.util.zookeeper import ZK


TEST_GROUP = 'test_group'
NEW_GROUP = 'new_group'


def call_rename_group(groupid):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'rename_group',
           TEST_GROUP,
           groupid]
    return call_cmd(cmd)


@when(u'we call the rename_group command with a new groupid')
def step_impl2(context):
    call_rename_group(NEW_GROUP)


@then(u'the committed offsets in the new group will match the expected values')
def step_impl4(context):
    cluster_config = get_cluster_config()
    with ZK(cluster_config) as zk:
        offsets = zk.get_group_offsets(TEST_GROUP)
        new_offsets = zk.get_group_offsets(NEW_GROUP)
    assert offsets is None
    assert context.topic in new_offsets
