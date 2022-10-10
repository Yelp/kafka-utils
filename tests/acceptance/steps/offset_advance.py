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

from kafka_utils.util.zookeeper import ZK


def call_offset_advance(groupid, topic=None, force=False):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_advance',
           groupid]
    if topic:
        cmd.extend(['--topic', topic])
    if force:
        cmd.extend(['--force'])
    return call_cmd(cmd)


@when('we call the offset_advance command and commit into kafka')
def step_impl3_2(context):
    call_offset_advance(
        context.group,
        topic=context.topic,
        force=True,
    )


@then('the committed offsets will match the latest message offsets')
def step_impl4(context):
    cluster_config = get_cluster_config()
    with ZK(cluster_config) as zk:
        offsets = zk.get_group_offsets(context.group)
    assert offsets[context.topic]["0"] == context.msgs_produced


@then('the latest message offsets will be shown')
def step_impl5_2(context):
    offset = context.msgs_produced
    pattern = 'Current Offset: {}'.format(offset)
    assert pattern in context.output
