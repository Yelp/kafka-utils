# Copyright 2018 Yelp Inc.
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


def call_replication_factor_check(min_isr='1'):
    cmd = ['kafka-check',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config', '-v',
           'replication_factor',
           '--default-min-isr', min_isr]
    return call_cmd(cmd)


@when('we call the replication_factor check')
def step_impl2(context):
    context.replication_factor_out = call_replication_factor_check()


@when('we call the replication_factor check with adjusted min.isr')
def step_impl3(context):
    context.replication_factor_out = call_replication_factor_check('0')


@then('OK from replication_factor will be printed')
def step_impl5(context):
    msg = 'OK: All topics have proper replication factor.\n'
    assert context.replication_factor_out == msg, context.replication_factor_out


@then('CRITICAL from replication_factor will be printed')
def step_impl6(context):
    msg = (
        "CRITICAL: 1 topic(s) have replication factor lower than specified min ISR + 1.\n"
        "Topics:\n"
        "replication_factor=1 is lower than min_isr=1 + 1 for {topic}\n"
    ).format(topic=context.topic)

    assert context.replication_factor_out == msg, context.replication_factor_out
