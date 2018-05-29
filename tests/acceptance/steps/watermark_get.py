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
from steps.util import call_watermark_get


@when(u'we call the watermark_get command')
def step_impl4(context):
    context.output = call_watermark_get(context.topic)


@then(u'the correct watermark will be shown')
def step_impl5(context):
    highmark = context.msgs_produced
    highmark_pattern = 'High Watermark: {}'.format(highmark)
    lowmark_pattern = 'Low Watermark: 0'
    assert highmark_pattern in context.output
    assert lowmark_pattern in context.output


@when(u'we call the watermark_get command with -r')
def step_impl6(context):
    context.output = call_watermark_get('.*bc.*', regex=True)


@when(u'we call the watermark_get command without -r')
def step_impl7(context):
    context.output = call_watermark_get('abcd')


@then(u'the correct two topics will be shown')
def step_impl8(context):
    topic1 = context.topic[0]
    topic2 = context.topic[1]
    assert topic1 in context.output
    assert topic2 in context.output


@then(u'the correct single topic will be shown')
def step_impl9(context):
    topic1 = context.topic[0]
    topic2 = context.topic[1]
    assert topic1 not in context.output
    assert topic2 in context.output
