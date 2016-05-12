# -*- coding: utf-8 -*-
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

from .util import call_cmd
from .util import get_cluster_config
from kafka_tools.util.zookeeper import ZK


def offsets_data(topic, offset):
    return '''{topic}.{partition}={offset}'''.format(
        topic=topic,
        partition='0',
        offset=offset,
    )


def call_offset_rewind(groupid, topic, storage=None, force=None):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_rewind',
           groupid,
           '--topic', topic]
    if storage:
        cmd.extend(['--storage', storage])
    if force:
        cmd.extend(['--force', force])
    return call_cmd(cmd)


@when(u'we call the offset_rewind command with a groupid and topic')
def step_impl3(context):
    call_offset_rewind(context.group, context.topic)


@when(u'we call the offset_rewind command and commit into kafka')
def step_impl3_2(context):
    call_offset_rewind(context.group, context.topic, storage='kafka')


@when(u'we call the offset_rewind command with a new groupid and the force option')
def step_impl2(context):
    context.group = 'offset_advance_created_group'
    call_offset_rewind(
        context.group,
        topic=context.topic,
        force='force',
    )


@then(u'the committed offsets will match the earliest message offsets')
def step_impl4(context):
    cluster_config = get_cluster_config()
    with ZK(cluster_config) as zk:
        offsets = zk.get_group_offsets(context.group)
    assert offsets[context.topic]["0"] == 0
