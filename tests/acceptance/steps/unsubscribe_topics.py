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


def call_unsubscribe_topics(groupid, storage=None):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'unsubscribe_topics',
           groupid]
    if storage:
        cmd.extend(['--storage', storage])
    return call_cmd(cmd)


@when('we call the unsubscribe_topics command with zookeeper storage')
def step_impl2(context):
    call_unsubscribe_topics(context.group, 'zookeeper')


@then(u'the committed offsets will no longer exist in zookeeper')
def step_impl4(context):
    cluster_config = get_cluster_config()
    with ZK(cluster_config) as zk:
        offsets = zk.get_group_offsets(context.group)
    assert context.topic not in offsets
