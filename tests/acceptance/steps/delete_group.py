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


def call_delete_group(groupid, storage=None):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'delete_group',
           groupid]
    if storage:
        cmd.extend(['--storage', storage])
    return call_cmd(cmd)


@when('we call the delete_group command')
def step_impl2(context):
    call_delete_group(context.group)


@when('we call the delete_group command with kafka storage')
def step_impl2_2(context):
    call_delete_group(context.group)


@then(u'the specified group will not be found')
def step_impl5(context):
    pattern = 'Error: Consumer Group ID {} does' \
              ' not exist.'.format(context.group)
    assert pattern in context.output


@then(u'the specified group will be found')
def step_impl5_2(context):
    pattern = 'Error: Consumer Group ID {} does' \
              ' not exist.'.format(context.group)
    assert pattern not in context.output
