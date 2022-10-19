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

from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.offsets import get_current_consumer_offsets

SET_OFFSET_KAFKA = 65


def offsets_data(topic, offset):
    return '''{topic}.{partition}={offset}'''.format(
        topic=topic,
        partition='0',
        offset=offset,
    )


def call_offset_set(groupid, offsets_data, force=False):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_set',
           groupid,
           offsets_data]
    if force:
        cmd.extend(['--force'])
    return call_cmd(cmd)


@when('we call the offset_set command and commit into kafka')
def step_impl2_2(context):
    if not hasattr(context, 'group'):
        context.group = 'test_kafka_offset_group'
    context.offsets = offsets_data(context.topic, SET_OFFSET_KAFKA)
    context.set_offset_kafka = SET_OFFSET_KAFKA
    call_offset_set(context.group, context.offsets)


@when('we call the offset_set command with a new groupid and the force option')
def step_impl2_3(context):
    if not hasattr(context, 'group'):
        context.group = 'offset_set_created_group'
    context.offsets = offsets_data(context.topic, SET_OFFSET_KAFKA)
    context.set_offset_kafka = SET_OFFSET_KAFKA
    call_offset_set(context.group, context.offsets, force=True)


@then('the committed offsets will match the specified offsets')
def step_impl3(context):
    config = get_cluster_config()
    context.client = KafkaToolClient(config.broker_list)
    offsets = get_current_consumer_offsets(
        context.client,
        context.group,
        [context.topic],
    )
    assert offsets[context.topic][0] == SET_OFFSET_KAFKA
