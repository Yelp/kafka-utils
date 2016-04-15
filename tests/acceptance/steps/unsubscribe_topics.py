from behave import then
from behave import when
from util import call_cmd


TEST_GROUP = 'test_group'


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
    '''
    call_unsubscribe_topics(TEST_GROUP)
    '''


@then(u'the committed offsets will no longer exist in zookeeper')
def step_impl4(context):
    '''
    cluster_config = get_cluster_config()
    with ZK(cluster_config) as zk:
        offsets = zk.get_group_offsets(TEST_GROUP)
    assert context.topic not in offsets
    '''
