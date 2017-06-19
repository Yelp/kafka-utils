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
from steps.util import call_offset_get
from steps.util import load_json


@when(u'we call the offset_get command')
def step_impl4(context):
    context.output = call_offset_get(context.group)


@when(u'we call the offset_get command with the dual storage option')
def step_impl4_2(context):
    context.output = call_offset_get(context.group, storage='dual')


@when(u'we call the offset_get command with kafka storage')
def step_impl4_3(context):
    context.output = call_offset_get(context.group)


@when(u'we call the offset_get command with zookeeper storage')
def step_impl4_4(context):
    context.output = call_offset_get(context.group, storage='zookeeper')


@when(u'we call the offset_get command with the json option with zookeeper storage')
def step_impl4_5(context):
    context.output = call_offset_get(context.group, storage='zookeeper', json=True)


@then(u'the correct offset will be shown')
def step_impl5(context):
    offset = context.msgs_consumed
    pattern = 'Current Offset: {}'.format(offset)
    assert pattern in context.output


@then(u'the offset that was committed into Kafka will be shown')
def step_impl5_2(context):
    offset = context.set_offset_kafka
    pattern = 'Current Offset: {}'.format(offset)
    assert pattern in context.output


@then(u'the correct json output will be shown')
def step_impl5_3(context):
    offset = context.msgs_consumed
    if context.msgs_produced > 0.0:
        percentage_distance = round((context.msgs_produced - offset) * 100.0 / context.msgs_produced, 2)
    else:
        percentage_distance = 0.0

    parsed_output = load_json(context.output)
    assert parsed_output == [
        {
            "topic": context.topic,
            "partition": 0,
            "current": offset,
            "highmark": context.msgs_produced,
            "lowmark": 0,
            "offset_distance": context.msgs_produced - offset,
            "percentage_distance": percentage_distance
        }
    ]
