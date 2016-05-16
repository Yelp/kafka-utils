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
from behave import given
from behave import when

from .util import create_consumer_group
from .util import create_random_topic
from .util import initialize_kafka_offsets_topic
from .util import produce_example_msg

TEST_GROUP = 'test_group'
PRODUCED_MSG_COUNT = 82
CONSUMED_MSG_COUNT = 39


@given(u'we have an existing kafka cluster with a topic')
def step_impl1(context):
    context.topic = create_random_topic(1, 1)


@when(u'we produce some number of messages into the topic')
def step_impl2(context):
    produce_example_msg(context.topic, num_messages=PRODUCED_MSG_COUNT)
    context.msgs_produced = PRODUCED_MSG_COUNT


@when(u'we consume some number of messages from the topic')
def step_impl3(context):
    context.consumer = create_consumer_group(
        context.topic,
        TEST_GROUP,
        num_messages=CONSUMED_MSG_COUNT,
    )
    context.group = TEST_GROUP
    context.msgs_consumed = CONSUMED_MSG_COUNT


@given(u'we have initialized kafka offsets storage')
def step_impl4(context):
    initialize_kafka_offsets_topic()


@given(u'we have an existing kafka cluster')
def step_impl5(context):
    pass
