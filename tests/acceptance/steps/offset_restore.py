import os
import subprocess

from behave import given
from behave import then
from behave import when
from util import create_random_topic

from yelp_kafka_tool.util import config
from yelp_kafka_tool.util.zookeeper import ZK


TEST_GROUP = 'test_group'
TEST_TOPIC = 'restore_test_topic'
RESTORED_OFFSET = 66
OFFSETS_FILE = 'offset_restore_data.json'


def offsets_file_path():
    script_dir = os.path.dirname(__file__)
    return os.path.join(script_dir, OFFSETS_FILE)


def call_offset_restore():
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_restore',
           offsets_file_path()]
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        output = e.output
    return output


def get_cluster_config():
    return config.get_cluster_config(
        'test',
        'test_cluster',
        'tests/acceptance/config',
    )


@given(u'we have a kafka cluster and a json offsets file')
def step_impl1(context):
    context.topic = create_random_topic(1, 1, TEST_TOPIC)
    assert os.path.isfile(offsets_file_path())


@when(u'we call the offset_restore command with the offsets file')
def step_impl2(context):
    context.output = call_offset_restore()


@then(u'the correct offsets will be commited')
def step_impl3(context):
    cluster_config = get_cluster_config()
    with ZK(cluster_config) as zk:
        offsets = zk.get_group_offsets(TEST_GROUP)
        assert offsets[TEST_TOPIC]["0"] == RESTORED_OFFSET
