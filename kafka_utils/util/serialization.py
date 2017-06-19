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
import json

import six


def load_json(input_data):
    if six.PY3:
        data_str = input_data.decode()
    else:
        data_str = input_data

    return json.loads(data_str)


def dump_json(obj):
    serialized = json.dumps(obj, sort_keys=True)

    if six.PY3:
        serialized = serialized.encode()

    return serialized
