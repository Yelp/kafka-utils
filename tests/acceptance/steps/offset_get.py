from behave import given
from behave import then
from behave import when
from util import call_cmd
from util import create_consumer_group
from util import create_random_topic
from util import produce_example_msg

TEST_GROUP = 'test_group'
PRODUCED_MSG_COUNT = 82
CONSUMED_MSG_COUNT = 39


@given(u'we have an existing kafka cluster with a topic')
def step_impl1(context):
    context.topic = create_random_topic(1, 1)


def call_offset_get():
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_get',
           TEST_GROUP]
    return call_cmd(cmd)


@when(u'we produce some number of messages into the topic')
def step_impl2(context):
    produce_example_msg(context.topic, num_messages=PRODUCED_MSG_COUNT)
    context.msgs_produced = PRODUCED_MSG_COUNT


@when(u'we consume some number of messages from the topic')
def step_impl3(context):
    context.consumer = create_consumer_group(
        context.topic,
        TEST_GROUP,
        num_messages=CONSUMED_MSG_COUNT,
    )
    context.msgs_consumed = CONSUMED_MSG_COUNT


@when(u'we call the offset_get command')
def step_impl4(context):
    context.output = call_offset_get()


@then(u'the correct offset will be shown')
def step_impl5(context):
    offsets = context.consumer.offsets(group='commit')
    key = (context.topic, 0)
    offset = offsets[key]
    pattern = 'Current Offset: {}'.format(offset)
    assert pattern in context.output
