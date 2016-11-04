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
from kafka_utils.util import tuple_alter
from kafka_utils.util import tuple_remove
from kafka_utils.util import tuple_replace


def test_tuple_alter():
    result = tuple_alter(
        (1, 2, 3, 2, 1),
        (2, lambda x: x + 1),
        (3, lambda x: x + 2),
        (4, lambda x: x + 3),
    )

    assert result == (1, 2, 4, 4, 4)


def test_tuple_remove():
    assert tuple_remove((1, 2, 3, 2, 1), 1, 2, 3) == (2, 1)


def test_tuple_replace():
    result = tuple_replace(
        (1, 2, 3, 2, 1),
        (2, 1),
        (3, 2),
        (4, 3),
    )

    assert result == (1, 2, 1, 2, 3)
