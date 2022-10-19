# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import tempfile

from behave import given
from behave import then
from behave import when
from steps.util import call_cmd
from steps.util import get_cluster_config

from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.offsets import get_current_consumer_offsets


RESTORED_OFFSET = 55


def create_restore_file(group, topic, offset):
    offset_restore_data = '''
    {{
    "groupid": "{group}",
    "offsets": {{
    "{topic}": {{
    "0": {offset}
    }}
    }}
    }}
    '''.format(group=group, topic=topic, offset=offset)

    f = tempfile.NamedTemporaryFile()

    offset_restore_data = offset_restore_data.encode()

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


@given('we have a json offsets file')
def step_impl2(context):
    context.restored_offset = RESTORED_OFFSET
    context.offsets_file = create_restore_file(
        context.group,
        context.topic,
        context.restored_offset,
    )
    assert os.path.isfile(context.offsets_file.name)


@when('we call the offset_restore command with the offsets file')
def step_impl3_2(context):
    call_offset_restore(context.offsets_file.name)


@then('the committed offsets will match the offsets file')
def step_impl4(context):
    config = get_cluster_config()
    context.client = KafkaToolClient(config.broker_list)
    offsets = get_current_consumer_offsets(
        context.client,
        context.group,
        [context.topic],
    )
    assert offsets[context.topic][0] == RESTORED_OFFSET
