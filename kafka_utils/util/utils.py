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
from __future__ import unicode_literals

import importlib
import inspect
import os
import sys


def get_module(module_full_name):
    if ':' in module_full_name:
        path, module_name = module_full_name.rsplit(':', 1)
        if not os.path.isdir(path):
            print("{0} is not a valid directory".format(path), file=sys.stderr)
            sys.exit(1)
        sys.path.append(path)
        return importlib.import_module(module_name)
    else:
        return importlib.import_module(module_full_name)


def dynamic_import(module_full_name, base_class):
    module = get_module(module_full_name)
    for _, class_type in inspect.getmembers(module, inspect.isclass):
        if (issubclass(class_type, base_class) and
                class_type is not base_class):
            return class_type
