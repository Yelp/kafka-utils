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
from steps.util import create_random_group_id
from steps.util import get_cluster_config

from kafka_utils.util.client import KafkaToolClient
from kafka_utils.util.monitoring import get_current_consumer_offsets
from kafka_utils.util.offsets import set_consumer_offsets


TEST_OFFSET = 56


def commit_offsets(offsets, group):
    # Setup the Kafka client
    config = get_cluster_config()
    client = KafkaToolClient(config.broker_list)
    set_consumer_offsets(
        client,
        group,
        offsets,
    )
    client.close()


def fetch_offsets(group, topics):
    # Setup the Kafka client
    config = get_cluster_config()
    client = KafkaToolClient(config.broker_list)
    offsets = get_current_consumer_offsets(client, group, topics, False)
    client.close()
    return offsets


@when(u'we commit some offsets for a group into kafka')
def step_impl4(context):
    context.offsets = {context.topic: {0: TEST_OFFSET}}
    context.group = create_random_group_id()
    commit_offsets(context.offsets, context.group)


@when(u'we fetch offsets for the group')
def step_impl4_3(context):
    topics = list(context.offsets.keys())
    context.fetched_offsets = fetch_offsets(context.group, topics)


@then(u'the fetched offsets will match the committed offsets')
def step_impl5(context):
    assert context.fetched_offsets == context.offsets
