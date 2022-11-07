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
from __future__ import annotations

import json
import sys
from argparse import ArgumentTypeError
from itertools import groupby
from typing import Any
from typing import Callable
from typing import Iterator
from typing import TypeVar
from typing import Union

from typing_extensions import Protocol

T = TypeVar('T')


def tuple_replace(tup: tuple[T, ...], *pairs: tuple[int, T]) -> tuple[T, ...]:
    """Return a copy of a tuple with some elements replaced.

    :param tup: The tuple to be copied.
    :param pairs: Any number of (index, value) tuples where index is the index
        of the item to replace and value is the new value of the item.
    """
    tuple_list = list(tup)
    for index, value in pairs:
        tuple_list[index] = value
    return tuple(tuple_list)


def tuple_alter(tup: tuple[T, ...], *pairs: tuple[int, Callable[[T], T]]) -> tuple[T, ...]:
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


def tuple_remove(tup: tuple[T, ...], *items: T) -> tuple[T, ...]:
    """Return a copy of a tuple with some items removed.

    :param tup: The tuple to be copied.
    :param items: Any number of items. The first instance of each item will
        be removed from the tuple.
    """
    tuple_list = list(tup)
    for item in items:
        tuple_list.remove(item)
    return tuple(tuple_list)


def positive_int(string: str) -> int:
    """Convert string to positive integer."""
    error_msg = f'Positive integer required, {string} given.'
    try:
        value = int(string)
    except ValueError:
        raise ArgumentTypeError(error_msg)
    if value < 0:
        raise ArgumentTypeError(error_msg)
    return value


def positive_nonzero_int(string: str) -> int:
    """Convert string to positive integer greater than zero."""
    error_msg = f'Positive non-zero integer required, {string} given.'
    try:
        value = int(string)
    except ValueError:
        raise ArgumentTypeError(error_msg)
    if value <= 0:
        raise ArgumentTypeError(error_msg)
    return value


def positive_float(string: str) -> float:
    """Convert string to positive float."""
    error_msg = f'Positive float required, {string} given.'
    try:
        value = float(string)
    except ValueError:
        raise ArgumentTypeError(error_msg)
    if value < 0:
        raise ArgumentTypeError(error_msg)
    return value


class SupportsLessThan(Protocol):
    def __lt__(self: T, __other: T) -> bool:
        ...


class SupportsGreaterThan(Protocol):
    def __gt__(self: T, __other: T) -> bool:
        ...


R = TypeVar('R', bound=Union[SupportsLessThan, SupportsGreaterThan])


def groupsortby(data: list[T], key: Callable[[T], R]) -> Iterator[tuple[R, Iterator[T]]]:
    """Sort and group by the same key."""
    return groupby(sorted(data, key=key), key)


V = TypeVar('V')


def dict_merge(set1: dict[T, V], set2: dict[T, V]) -> dict[T, V]:
    """Joins two dictionaries."""
    return dict(list(set1.items()) + list(set2.items()))


def to_h(num: float | None, suffix: str = 'B') -> str:
    """Converts a byte value in human readable form."""
    if num is None:  # Show None when data is missing
        return "None"
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return "{:.1f}{}{}".format(num, 'Yi', suffix)


def to_int(num: int | None) -> str:
    """
    Converts 'num' to int representation in string
    or to "None" in case of None.
    """
    if num is None:
        return "None"
    return f"{num:.0f}"


def to_float(num: float | None) -> str:
    """
    Converts 'num' to float representation in string
    or to "None" in case of None.
    """
    if num is None:
        return "None"
    return f"{num:.2f}"


def format_to_json(data: Any) -> str:
    """Converts `data` into json
    If stdout is a tty it performs a pretty print.
    """
    if sys.stdout.isatty():
        return json.dumps(data, indent=4, separators=(',', ': '))
    else:
        return json.dumps(data)


def print_json(data: Any) -> None:
    """Converts `data` into json and prints it to stdout."""
    print(format_to_json(data))
