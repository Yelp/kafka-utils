from behave import then
from behave import when
from util import call_cmd


def call_offset_get(group):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_get',
           group]
    return call_cmd(cmd)


@when(u'we call the offset_get command')
def step_impl4(context):
    context.output = call_offset_get(context.group)


@then(u'the correct offset will be shown')
def step_impl5(context):
    offsets = context.consumer.offsets(group='commit')
    key = (context.topic, 0)
    offset = offsets[key]
    pattern = 'Current Offset: {}'.format(offset)
    assert pattern in context.output
