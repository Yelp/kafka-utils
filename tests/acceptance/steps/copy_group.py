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
from steps.util import call_cmd

from kafka_utils.util.offsets import get_current_consumer_offsets


NEW_GROUP = 'new_group'


def call_copy_group(old_group, new_group):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'copy_group',
           old_group,
           new_group]
    return call_cmd(cmd)


@when(u'we call the copy_group command with a new groupid')
def step_impl3(context):
    call_copy_group(context.group, NEW_GROUP)


@then('the committed offsets in kafka for the new group will match the old group')
def step_impl4(context):
    old_group_offsets = get_current_consumer_offsets(
        context.client,
        context.group,
        [context.topic],
    )
    new_group_offsets = get_current_consumer_offsets(
        context.client,
        NEW_GROUP,
        [context.topic],
    )
    assert old_group_offsets == new_group_offsets
