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
from __future__ import print_function

import json
import sys
from argparse import ArgumentTypeError
from itertools import groupby


def tuple_replace(tup, *pairs):
    """Return a copy of a tuple with some elements replaced.

    :param tup: The tuple to be copied.
    :param pairs: Any number of (index, value) tuples where index is the index
        of the item to replace and value is the new value of the item.
    """
    tuple_list = list(tup)
    for index, value in pairs:
        tuple_list[index] = value
    return tuple(tuple_list)


def tuple_alter(tup, *pairs):
    """Return a copy of a tuple with some elements altered.

    :param tup: The tuple to be copied.
    :param pairs: Any number of (index, func) tuples where index is the index
        of the item to alter and the new value is func(tup[index]).
    """
    # timeit says that this is faster than a similar
    tuple_list = list(tup)
    for i, f in pairs:
        tuple_list[i] = f(tuple_list[i])
    return tuple(tuple_list)


def tuple_remove(tup, *items):
    """Return a copy of a tuple with some items removed.

    :param tup: The tuple to be copied.
    :param items: Any number of items. The first instance of each item will
        be removed from the tuple.
    """
    tuple_list = list(tup)
    for item in items:
        tuple_list.remove(item)
    return tuple(tuple_list)


def positive_int(string):
    """Convert string to positive integer."""
    error_msg = 'Positive integer required, {string} given.'.format(string=string)
    try:
        value = int(string)
    except ValueError:
        raise ArgumentTypeError(error_msg)
    if value < 0:
        raise ArgumentTypeError(error_msg)
    return value


def positive_nonzero_int(string):
    """Convert string to positive integer greater than zero."""
    error_msg = 'Positive non-zero integer required, {string} given.'.format(string=string)
    try:
        value = int(string)
    except ValueError:
        raise ArgumentTypeError(error_msg)
    if value <= 0:
        raise ArgumentTypeError(error_msg)
    return value


def positive_float(string):
    """Convert string to positive float."""
    error_msg = 'Positive float required, {string} given.'.format(string=string)
    try:
        value = float(string)
    except ValueError:
        raise ArgumentTypeError(error_msg)
    if value < 0:
        raise ArgumentTypeError(error_msg)
    return value


def groupsortby(data, key):
    """Sort and group by the same key."""
    return groupby(sorted(data, key=key), key)


def dict_merge(set1, set2):
    """Joins two dictionaries."""
    return dict(list(set1.items()) + list(set2.items()))


def to_h(num, suffix='B'):
    """Converts a byte value in human readable form."""
    if num is None:  # Show None when data is missing
        return "None"
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def to_int(num):
    """
    Converts 'num' to int representation in string
    or to "None" in case of None.
    """
    if num is None:
        return "None"
    return "{:.0f}".format(num)


def to_float(num):
    """
    Converts 'num' to float representation in string
    or to "None" in case of None.
    """
    if num is None:
        return "None"
    return "{:.2f}".format(num)


def format_to_json(data):
    """Converts `data` into json
    If stdout is a tty it performs a pretty print.
    """
    if sys.stdout.isatty():
        return json.dumps(data, indent=4, separators=(',', ': '))
    else:
        return json.dumps(data)


def print_json(data):
    """Converts `data` into json and prints it to stdout."""
    print(format_to_json(data))
