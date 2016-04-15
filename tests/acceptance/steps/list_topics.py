from behave import given
from behave import then
from behave import when
from util import call_cmd
from util import create_consumer_group
from util import create_random_topic
from util import produce_example_msg

test_groups = ['group1', 'group2', 'group3']
test_topics = ['topic1', 'topic2']


@given('we have a set of existing consumer groups and topics')
def step_impl1(context):
    for topic in test_topics:
        create_random_topic(1, 1, topic_name=topic)
        produce_example_msg(topic)

    for group in test_groups:
        create_consumer_group(topic, group)


def call_list_topics(groupid):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'list_topics',
           groupid]
    return call_cmd(cmd)


@when('we call the list_topics command')
def step_impl2(context):
    context.output = call_list_topics('group1')


@then('the topics will be listed')
def step_impl3(context):
    for topic in test_topics:
        assert topic in context.output
