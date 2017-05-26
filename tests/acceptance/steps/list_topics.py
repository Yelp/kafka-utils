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

from behave import given
from behave import then
from behave import when

from .util import call_cmd
from .util import create_consumer_group
from .util import create_consumer_group_with_kafka_storage
from .util import create_random_topic
from .util import produce_example_msg

test_group = 'group1'
test_topics = ['topic1', 'topic2', 'topic3']


@given('we have a set of existing topics and a consumer group')
def step_impl1(context):
    for topic in test_topics:
        create_random_topic(1, 1, topic_name=topic)
        produce_example_msg(topic)

        create_consumer_group(topic, test_group)


@given('we have a set of existing topics and a consumer group with default storage')
def step_impl2(context):
    for topic in test_topics:
        create_random_topic(1, 1, topic_name=topic)
        produce_example_msg(topic)

        create_consumer_group_with_kafka_storage(topic, test_group)


def call_list_topics(groupid, storage=None):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'list_topics',
           groupid]
    if storage:
        cmd.extend(['--storage', storage])
    return call_cmd(cmd)


@when('we call the list_topics command with default storage')
def step_impl3(context):
    context.output = call_list_topics('group1')


@when('we call the list_topics command with zookeeper storage')
def step_impl4(context):
    context.output = call_list_topics('group1', 'zookeeper')


@then('the topics will be listed')
def step_impl5(context):
    for topic in test_topics:
        assert topic in context.output
