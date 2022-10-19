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
from behave import then
from behave import when
from steps.util import call_cmd
from steps.util import get_cluster_config
from steps.util import set_consumer_group_offset

from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.offsets import get_current_consumer_offsets


def offsets_data(topic, offset):
    return '''{topic}.{partition}={offset}'''.format(
        topic=topic,
        partition='0',
        offset=offset,
    )


def call_offset_rewind(groupid, topic, force=False):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_rewind',
           groupid,
           '--topic', topic]
    if force:
        cmd.extend(['--force'])
    return call_cmd(cmd)


@when('we set the offsets to a high number to emulate consumption')
def step_impl2(context):
    set_consumer_group_offset(
        topic=context.topic,
        group=context.group,
        offset=100,
    )


@when('we call the offset_rewind command and commit into kafka')
def step_impl3(context):
    call_offset_rewind(context.group, context.topic)


@when('we call the offset_rewind command with a new groupid and the force option')
def step_impl4(context):
    context.group = 'offset_rewind_created_group'
    call_offset_rewind(
        context.group,
        topic=context.topic,
        force=True,
    )


@then('the committed offsets will match the earliest message offsets')
def step_impl5(context):
    config = get_cluster_config()
    context.client = KafkaToolClient(config.broker_list)
    offsets = get_current_consumer_offsets(
        context.client,
        context.group,
        [context.topic],
    )
    assert offsets[context.topic][0] == 0


@then('consumer_group wont exist since it is rewind to low_offset 0')
def step_impl6(context):
    # Since using kafka offset storage, lowmark is 0, then a rewind to lowmark
    # will remove the consumer group from kafka
    assert "Current Offset" not in context.output


@then('the earliest message offsets will be shown')
def step_impl7(context):
    offset = 0
    pattern = f'Current Offset: {offset}'
    assert pattern in context.output
