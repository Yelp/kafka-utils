import subprocess

from behave import given
from behave import then
from behave import when
from kafka import KafkaConsumer
from util import create_random_topic
from util import KAFKA_URL
from util import produce_example_msg

TEST_GROUP = 'test_group'
MSG_COUNT = 50


def create_consumer(topic, group_name):
    return KafkaConsumer(
        topic,
        group_id=group_name,
        auto_commit_enable=False,
        bootstrap_servers=[KAFKA_URL],
        auto_offset_reset='smallest')


def consume_and_set_offset(consumer):
    for i in xrange(MSG_COUNT):
        message = consumer.next()
        consumer.task_done(message)
    consumer.commit()


@given(u'we have an existing consumer group for a topic in the kafka cluster')
def step_impl1(context):
    topic = create_random_topic(1, 1)
    for i in xrange(MSG_COUNT):
        produce_example_msg(topic)

    consumer = create_consumer(topic, TEST_GROUP)
    consume_and_set_offset(consumer)
    context.topic = topic
    context.consumer = consumer


def call_offset_get():
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_get',
           TEST_GROUP]
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        output = e.output
    return output


@when(u'we call the offset_get command')
def step_impl2(context):
    context.output = call_offset_get()


@then(u'the correct offset  will be shown')
def step_impl3(context):
    offsets = context.consumer.offsets(group='commit')
    key = (context.topic, 0)
    offset = offsets[key]
    pattern = 'Current Offset: {}'.format(offset)
    assert pattern in context.output
