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


def call_offset_get(group, storage=None):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_get',
           group]
    if storage:
        cmd.extend(['--storage', storage])
    return call_cmd(cmd)


@when(u'we call the offset_get command')
def step_impl4(context):
    context.output = call_offset_get(context.group)


@when(u'we call the offset_get command with the dual storage option')
def step_impl4_2(context):
    context.output = call_offset_get(context.group, storage='dual')


@when(u'we call the offset_get command with kafka storage')
def step_impl4_3(context):
    context.output = call_offset_get(context.group, storage='kafka')


@when(u'we call the offset_get command with zookeeper storage')
def step_impl4_4(context):
    context.output = call_offset_get(context.group, storage='zookeeper')


@then(u'the correct offset will be shown')
def step_impl5(context):
    offsets = context.consumer.offsets(group='commit')
    key = (context.topic, 0)
    offset = offsets[key]
    pattern = 'Current Offset: {}'.format(offset)
    assert pattern in context.output


@then(u'the offset that was committed into Kafka will be shown')
def step_impl5_2(context):
    offset = context.set_offset_kafka
    pattern = 'Current Offset: {}'.format(offset)
    assert pattern in context.output
