import json
import tempfile

from behave import then
from behave import when
from util import call_cmd

from yelp_kafka_tool.util import config


TEST_GROUP = 'test_group'
TEST_TOPIC = 'save_test_topic'
SAVED_OFFSET = 77


def create_saved_file():
    return tempfile.NamedTemporaryFile()


def call_offset_save(offsets_file):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_save',
           TEST_GROUP,
           offsets_file]
    return call_cmd(cmd)


def get_cluster_config():
    return config.get_cluster_config(
        'test',
        'test_cluster',
        'tests/acceptance/config',
    )


@when(u'we call the offset_save command with an offsets file')
def step_impl2(context):
    context.offsets_file = create_saved_file()
    call_offset_save(context.offsets_file.name)


@then(u'the correct offsets will be saved into the given file')
def step_impl3(context):
    data = json.loads(context.offsets_file.read())
    assert context.msgs_consumed == data['offsets'][context.topic]['0']
    context.offsets_file.close()
