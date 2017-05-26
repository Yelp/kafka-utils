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


def call_replica_unavailability():
    cmd = ['kafka-check',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'replica_unavailability']
    return call_cmd(cmd)


@when(u'we call the replica_unavailability command')
def step_impl2(context):
    context.replica_unavailability_out = call_replica_unavailability()


@then(u'OK replica_unavailability will be printed')
def step_impl5(context):
    assert context.replica_unavailability_out == 'OK: All replicas available for communication.\n', context.replica_unavailability_out
