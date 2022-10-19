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
from argparse import ArgumentTypeError

import pytest

from kafka_utils.util import positive_float
from kafka_utils.util import positive_int
from kafka_utils.util import positive_nonzero_int
from kafka_utils.util import tuple_alter
from kafka_utils.util import tuple_remove
from kafka_utils.util import tuple_replace
from kafka_utils.util.utils import child_class


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


def test_positive_int_valid():
    assert positive_int('123') == 123


def test_positive_int_not_int():
    with pytest.raises(ArgumentTypeError):
        positive_int('not_an_int')


def test_positive_int_negative_int():
    with pytest.raises(ArgumentTypeError):
        positive_int('-5')


def test_positive_nonzero_int_valid():
    assert positive_nonzero_int('123') == 123


def test_positive_nonzero_int_not_int():
    with pytest.raises(ArgumentTypeError):
        positive_nonzero_int('not_an_int')


def test_positive_nonzero_int_zero():
    with pytest.raises(ArgumentTypeError):
        positive_nonzero_int('0')


def test_positive_float_valid():
    assert positive_float('123.0') == 123.0


def test_positive_float_not_float():
    with pytest.raises(ArgumentTypeError):
        positive_float('not_a_float')


def test_positive_float_negative_float():
    with pytest.raises(ArgumentTypeError):
        positive_float('-1.45')


def test_child_class():
    class A:
        pass

    class B(A):
        pass

    class C(B):
        pass

    class D(A):
        pass

    assert child_class([A, B, C], A) == C
    assert child_class([A, B], A) == B
    assert child_class([A, B, D], A) in {B, D}
