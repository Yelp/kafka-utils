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
from __future__ import absolute_import

from behave import then
from behave import when

from .util import call_cmd
from .util import get_cluster_config
from kafka_utils.util.zookeeper import ZK


SET_OFFSET = 36
SET_OFFSET_KAFKA = 65


def offsets_data(topic, offset):
    return '''{topic}.{partition}={offset}'''.format(
        topic=topic,
        partition='0',
        offset=offset,
    )


def call_offset_set(groupid, offsets_data, storage=None, force=False):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_set',
           groupid,
           offsets_data]
    if storage:
        cmd.extend(['--storage', storage])
    if force:
        cmd.extend(['--force'])
    return call_cmd(cmd)


@when(u'we call the offset_set command with a groupid and offset data')
def step_impl2(context):
    context.offsets = offsets_data(context.topic, SET_OFFSET)
    call_offset_set(context.group, context.offsets, storage='zookeeper')


@when(u'we call the offset_set command and commit into kafka')
def step_impl2_2(context):
    if not hasattr(context, 'group'):
        context.group = 'test_kafka_offset_group'
    context.offsets = offsets_data(context.topic, SET_OFFSET_KAFKA)
    context.set_offset_kafka = SET_OFFSET_KAFKA
    call_offset_set(context.group, context.offsets)


@when(u'we call the offset_set command with a new groupid and the force option')
def step_impl2_3(context):
    context.offsets = offsets_data(context.topic, SET_OFFSET)
    context.group = 'offset_set_created_group'
    call_offset_set(context.group, context.offsets, storage='zookeeper', force=True)


@then(u'the committed offsets will match the specified offsets')
def step_impl3(context):
    cluster_config = get_cluster_config()
    with ZK(cluster_config) as zk:
        offsets = zk.get_group_offsets(context.group)
    assert offsets[context.topic]["0"] == SET_OFFSET
