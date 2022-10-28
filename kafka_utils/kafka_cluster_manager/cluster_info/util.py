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

from collections.abc import Collection
from typing import Any
from typing import Callable
from typing import TypeVar


def compute_optimum(groups: int, elements: int) -> tuple[int, int]:
    """Compute the number of elements per group and the remainder.

        :param elements: total number of elements
        :param groups: total number of groups
    """
    return elements // groups, elements % groups


T = TypeVar('T')


def _smart_separate_groups(groups: Collection[T], key: Callable[[T], Any], total: int) -> tuple[list[T], list[T], list[T]]:
    """Given a list of group objects, and a function to extract the number of
    elements for each of them, return the list of groups that have an excessive
    number of elements (when compared to a uniform distribution), a list of
    groups with insufficient elements, and a list of groups that already have
    the optimal number of elements.

    :param list groups: list of group objects
    :param func key: function to retrieve the current number of elements from the group object
    :param int total: total number of elements to distribute

    Example:
        .. code-block:: python
           smart_separate_groups([11,  9, 10, 14], lambda g: g) => ([14], [10, 9], [11])
    """
    optimum, extra = compute_optimum(len(groups), total)
    over_loaded, under_loaded, optimal = [], [], []
    for group in sorted(groups, key=key, reverse=True):
        n_elements = key(group)
        additional_element = 1 if extra else 0
        if n_elements > optimum + additional_element:
            over_loaded.append(group)
        elif n_elements == optimum + additional_element:
            optimal.append(group)
        elif n_elements < optimum + additional_element:
            under_loaded.append(group)
        extra -= additional_element
    return over_loaded, under_loaded, optimal


def separate_groups(groups: Collection[T], key: Callable[[T], Any], total: int) -> tuple[list[T], list[T]]:
    """Separate the group into overloaded and under-loaded groups.

    The revised over-loaded groups increases the choice space for future
    selection of most suitable group based on search criteria.

    For example:
    Given the groups (a:4, b:4, c:3, d:2) where the number represents the number
    of elements for each group.
    smart_separate_groups sets 'a' and 'c' as optimal, 'b' as over-loaded
    and 'd' as under-loaded.

    separate-groups combines 'a' with 'b' as over-loaded, allowing to select
    between these two groups to transfer the element to 'd'.

    :param groups: list of groups
    :param key: function to retrieve element count from group
    :param total: total number of elements to distribute
    :returns: sorted lists of over loaded (descending) and under
        loaded (ascending) group
    """
    optimum, extra = compute_optimum(len(groups), total)
    over_loaded, under_loaded, optimal = _smart_separate_groups(groups, key, total)
    # If every group is optimal return
    if not extra:
        return over_loaded, under_loaded
    # Some groups in optimal may have a number of elements that is optimum + 1.
    # In this case they should be considered over_loaded.
    potential_under_loaded = [
        group for group in optimal
        if key(group) == optimum
    ]
    potential_over_loaded = [
        group for group in optimal
        if key(group) > optimum
    ]
    revised_under_loaded = under_loaded + potential_under_loaded
    revised_over_loaded = over_loaded + potential_over_loaded
    return (
        sorted(revised_over_loaded, key=key, reverse=True),
        sorted(revised_under_loaded, key=key),
    )
