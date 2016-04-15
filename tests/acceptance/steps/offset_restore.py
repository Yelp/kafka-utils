import os
import tempfile

from behave import given
from behave import then
from behave import when
from util import call_cmd
from util import get_cluster_config

from yelp_kafka_tool.util.zookeeper import ZK


TEST_GROUP = 'test_group'
RESTORED_OFFSET = 55


def create_restore_file(topic):
    offset_restore_data = '''
    {{
    "groupid": "{group}",
    "offsets": {{
    "{topic}": {{
    "0": {offset}
    }}
    }}
    }}
    '''.format(group=TEST_GROUP, topic=topic, offset=RESTORED_OFFSET)
    f = tempfile.NamedTemporaryFile()
    f.write(offset_restore_data)
    f.flush()
    return f


def call_offset_restore(offsets_file):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_restore',
           offsets_file]
    return call_cmd(cmd)


@given(u'we have a json offsets file')
def step_impl2(context):
    context.offsets_file = create_restore_file(context.topic)
    assert os.path.isfile(context.offsets_file.name)


@when(u'we call the offset_restore command with the offsets file')
def step_impl3(context):
    call_offset_restore(context.offsets_file.name)


@then(u'the committed offsets will match the offsets file')
def step_impl4(context):
    cluster_config = get_cluster_config()
    with ZK(cluster_config) as zk:
        offsets = zk.get_group_offsets(TEST_GROUP)
    assert offsets[context.topic]["0"] == RESTORED_OFFSET
    context.offsets_file.close()
