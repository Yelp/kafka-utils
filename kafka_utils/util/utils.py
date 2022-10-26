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

import importlib
import inspect
import os
import sys
from types import ModuleType
from typing import Collection


def get_module(module_full_name: str) -> ModuleType:
    if ':' in module_full_name:
        path, module_name = module_full_name.rsplit(':', 1)
        if not os.path.isdir(path):
            print(f"{path} is not a valid directory", file=sys.stderr)
            sys.exit(1)
        sys.path.append(path)
        return importlib.import_module(module_name)
    else:
        return importlib.import_module(module_full_name)


def child_class(class_types: Collection[type], base_class: type) -> type | None:
    """
    Find the child-most class of `base_class`.

    Examples:
        class A:
            pass

        class B(A):
            pass

        class C(B):
            pass

        child_class([A, B, C], A) = C
    """
    subclasses = set()
    for class_type in class_types:
        if class_type is base_class:
            continue
        if issubclass(class_type, base_class):
            subclasses.add(class_type)

    if len(subclasses) == 0:
        return None
    elif len(subclasses) == 1:
        return subclasses.pop()
    else:
        # If more than one class is a subclass of `base_class`
        # It is possible that one or more classes are subclasses of another
        # class (see example above).
        # Recursively find the child-most class. Break ties by returning any
        # child-most class.
        for c in subclasses:
            child = child_class(subclasses, c)
            if child is not None:
                return child
        return subclasses.pop()


def dynamic_import(module_full_name: str, base_class: type) -> type | None:
    module = get_module(module_full_name)
    class_types = [
        class_type
        for _, class_type in inspect.getmembers(module, inspect.isclass)
    ]
    return child_class(class_types, base_class)
