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
from util import call_cmd
from util import get_cluster_config

from kafka_utils.util.zookeeper import ZK


ISR_CONF_NAME = 'min.insync.replicas'
CONF_PATH = '/config/topics/'


def call_min_isr():
    cmd = ['kafka-check',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'min_isr']
    return call_cmd(cmd)


def set_min_isr(topic, min_isr):
    cluster_config = get_cluster_config()
    with ZK(cluster_config) as zk:
        config = zk.get_topic_config(topic)
        config['config'] = {ISR_CONF_NAME: str(min_isr)}
        zk.set_topic_config(topic, config)


@when(u'we call the min_isr command')
def step_impl2(context):
    context.min_isr_out = call_min_isr()


@when(u'we change min.isr settings for a topic to 1')
def step_impl3(context):
    set_min_isr(context.topic, 1)


@when(u'we change min.isr settings for a topic to 2')
def step_impl4(context):
    set_min_isr(context.topic, 2)


@then(u'OK min_isr will be printed')
def step_impl5(context):
    assert context.min_isr_out == 'OK: All replicas in sync.\n', context.min_isr_out


@then(u'CRITICAL min_isr will be printed')
def step_impl6(context):
    error_msg = ("CRITICAL: 1 partition(s) have the number of "
                 "replicas in sync that is lower than the specified min ISR.\n").format(
        topic=context.topic)
    assert context.min_isr_out == error_msg, context.min_isr_out
