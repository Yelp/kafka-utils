import os
import tempfile

from behave import given
from behave import then
from behave import when
from util import call_cmd
from util import create_random_topic

from yelp_kafka_tool.util import config
from yelp_kafka_tool.util.zookeeper import ZK


TEST_GROUP = 'test_group'
TEST_TOPIC = 'restore_test_topic'
RESTORED_OFFSET = 55

offset_restore_data = '''
{{
"groupid": "{group}",
"offsets": {{
"{topic}": {{
"0": {offset}
}}
}}
}}
'''.format(group=TEST_GROUP, topic=TEST_TOPIC, offset=RESTORED_OFFSET)


def create_restore_file():
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


def get_cluster_config():
    return config.get_cluster_config(
        'test',
        'test_cluster',
        'tests/acceptance/config',
    )


@given(u'we have a kafka cluster and a json offsets file')
def step_impl1(context):
    context.topic = create_random_topic(1, 1, TEST_TOPIC)

    context.offsets_file = create_restore_file()
    assert os.path.isfile(context.offsets_file.name)


@when(u'we call the offset_restore command with the offsets file')
def step_impl2(context):
    call_offset_restore(context.offsets_file.name)


@then(u'the correct offsets will be commited')
def step_impl3(context):
    cluster_config = get_cluster_config()
    with ZK(cluster_config) as zk:
        offsets = zk.get_group_offsets(TEST_GROUP)
    assert offsets[TEST_TOPIC]["0"] == RESTORED_OFFSET
    context.offsets_file.close()
