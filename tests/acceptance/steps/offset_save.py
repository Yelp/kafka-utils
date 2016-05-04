import json
import os
import tempfile

from behave import then
from behave import when

from offset_restore import RESTORED_OFFSET
from util import call_cmd


def create_saved_file():
    return tempfile.NamedTemporaryFile()


def call_offset_save(groupid, offsets_file, storage=None):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_save',
           groupid,
           offsets_file]
    if storage:
        cmd.extend(['--storage', storage])
    return call_cmd(cmd)


@when(u'we call the offset_save command with an offsets file')
def step_impl2(context):
    context.offsets_file = create_saved_file()
    call_offset_save(context.group, context.offsets_file.name)


@when(u'we call the offset_save command with an offsets file and kafka storage')
def step_impl2_2(context):
    if '0.9.0' == os.environ['KAFKA_VERSION']:
        context.offsets_file = create_saved_file()
        call_offset_save(context.group, context.offsets_file.name, storage='kafka')


@then(u'the correct offsets will be saved into the given file')
def step_impl3(context):
    offsets = context.consumer.offsets(group='commit')
    key = (context.topic, 0)
    offset = offsets[key]

    data = json.loads(context.offsets_file.read())
    assert offset == data['offsets'][context.topic]['0']
    context.offsets_file.close()


@then(u'the restored offsets will be saved into the given file')
def step_impl3_2(context):
    if '0.9.0' == os.environ['KAFKA_VERSION']:
        offset = RESTORED_OFFSET

        data = json.loads(context.offsets_file.read())
        assert offset == data['offsets'][context.topic]['0']
        context.offsets_file.close()
