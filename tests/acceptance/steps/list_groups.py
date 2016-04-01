import subprocess

from behave import given
from behave import then
from behave import when
from util import create_consumer_group
from util import create_random_topic
from util import produce_example_msg

test_groups = ['group1', 'group2', 'group3']


@given('we have a set of existing consumer groups')
def step_impl1(context):
    topic = create_random_topic(1, 1)
    produce_example_msg(topic)

    for group in test_groups:
        create_consumer_group(topic, group)
    context.topic = topic


def call_list_groups():
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'list_groups']
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        output = e.output
    return output


@when('we call the list_groups command')
def step_impl2(context):
    context.output = call_list_groups()


@then('the groups will be listed')
def step_impl3(context):
    for group in test_groups:
        assert group in context.output
