import json
import tempfile

from behave import then
from behave import when
from util import call_cmd


TEST_GROUP = 'test_group'
TEST_TOPIC = 'save_test_topic'


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


@when(u'we call the offset_save command with an offsets file')
def step_impl2(context):
    context.offsets_file = create_saved_file()
    call_offset_save(context.offsets_file.name)


@then(u'the correct offsets will be saved into the given file')
def step_impl3(context):
    offsets = context.consumer.offsets(group='commit')
    key = (context.topic, 0)
    offset = offsets[key]

    data = json.loads(context.offsets_file.read())
    assert offset == data['offsets'][context.topic]['0']
    context.offsets_file.close()
