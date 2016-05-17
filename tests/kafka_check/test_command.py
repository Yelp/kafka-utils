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
from kafka_utils.kafka_check.commands.command import parse_meta_properties_file


META_PROPERTIES_CONTENT = [
    "#\n",
    "#Thu May 12 17:59:12 BST 2016\n",
    "version=0\n",
    "broker.id=123\n",
]


class TestParseMetaPropertiesFile(object):

    def test_parse_meta_properties_file(self):
        broker_id = parse_meta_properties_file(META_PROPERTIES_CONTENT)

        assert broker_id == 123

    def test_parse_meta_properties_file_empty(self):
        broker_id = parse_meta_properties_file([])

        assert broker_id is None
