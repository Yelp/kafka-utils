from behave import then
from behave import when

from .util import call_cmd


def call_delete_group(groupid, storage=None):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'delete_group',
           groupid]
    if storage:
        cmd.extend(['--storage', storage])
    return call_cmd(cmd)


@when('we call the delete_group command')
def step_impl2(context):
    call_delete_group(context.group)


@when('we call the delete_group command with kafka storage')
def step_impl2_2(context):
    call_delete_group(context.group)


@then(u'the specified group will not be found')
def step_impl5(context):
    pattern = 'Error: Consumer Group ID {} does' \
              ' not exist.'.format(context.group)
    assert pattern in context.output
