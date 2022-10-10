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
from steps.util import call_offset_get


NEW_GROUP = 'new_group'


def call_rename_group(old_group, new_group):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'rename_group',
           old_group,
           new_group]
    return call_cmd(cmd)


@when('we call the rename_group command')
def step_impl2(context):
    call_rename_group(context.group, NEW_GROUP)


@then('the committed offsets in the new group will match the expected values')
def step_impl3(context):
    new_group = call_offset_get(NEW_GROUP, True)
    old_group = call_offset_get(context.group, True)
    assert "does not exist" in old_group
    assert context.topic in new_group


@then('the group named has been changed')
def step_impl4(context):
    new_groups = call_offset_get(NEW_GROUP)
    old_group = call_offset_get(context.group)
    assert "Offset Distance" in new_groups
    assert "Offset Distance" not in old_group
