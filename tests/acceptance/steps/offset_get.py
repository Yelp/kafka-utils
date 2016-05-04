import os

from behave import then
from behave import when

from .util import call_cmd


def call_offset_get(group, storage=None):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_get',
           group]
    if storage:
        cmd.extend(['--storage', storage])
    return call_cmd(cmd)


@when(u'we call the offset_get command')
def step_impl4(context):
    context.output = call_offset_get(context.group)


@when(u'we call the offset_get command with the dual storage option')
def step_impl4_2(context):
    if '0.9.0' == os.environ['KAFKA_VERSION']:
        context.output = call_offset_get(context.group, storage='dual')


@then(u'the correct offset will be shown')
def step_impl5(context):
    offsets = context.consumer.offsets(group='commit')
    key = (context.topic, 0)
    offset = offsets[key]
    pattern = 'Current Offset: {}'.format(offset)
    assert pattern in context.output


@then(u'the offset that was committed into Kafka will be shown')
def step_impl5_2(context):
    if '0.9.0' == os.environ['KAFKA_VERSION']:
        offset = context.set_offset_kafka
        pattern = 'Current Offset: {}'.format(offset)
        assert pattern in context.output
