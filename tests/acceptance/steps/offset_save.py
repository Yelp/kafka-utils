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

import tempfile

from behave import then
from behave import when
from steps.util import call_cmd
from steps.util import load_json


def create_saved_file():
    return tempfile.NamedTemporaryFile()


def call_offset_save(groupid, offsets_file, storage=None):
    cmd = ['kafka-consumer-manager',
           '--cluster-type', 'test',
           '--cluster-name', 'test_cluster',
           '--discovery-base-path', 'tests/acceptance/config',
           'offset_save',
           groupid,
           offsets_file]
    if storage:
        cmd.extend(['--storage', storage])
    return call_cmd(cmd)


@when(u'we call the offset_save command with an offsets file and zookeeper storage')
def step_impl2(context):
    context.offsets_file = create_saved_file()
    call_offset_save(context.group, context.offsets_file.name, storage="zookeeper")


@when(u'we call the offset_save command with an offsets file and default storage')
def step_impl2_2(context):
    context.offsets_file = create_saved_file()
    call_offset_save(context.group, context.offsets_file.name)


@then(u'the correct offsets will be saved into the given file')
def step_impl3(context):
    offset = context.msgs_consumed

    data = load_json(context.offsets_file.read())
    assert offset == data['offsets'][context.topic]['0']
    context.offsets_file.close()


@then(u'the restored offsets will be saved into the given file')
def step_impl3_2(context):
    offset = context.restored_offset

    data = load_json(context.offsets_file.read())
    assert offset == data['offsets'][context.topic]['0']
    context.offsets_file.close()
