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
def step_impl2(context):
    call_copy_group(context.group, NEW_GROUP)


@then(u'the committed offsets in the new group will match the old group')
def step_impl4(context):
    cluster_config = get_cluster_config()
    with ZK(cluster_config) as zk:
        offsets = zk.get_group_offsets(context.group)
        new_offsets = zk.get_group_offsets(NEW_GROUP)
    assert context.topic in offsets
    assert new_offsets == offsets
