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
import shlex


class PrecheckFailedException(Exception):
    pass


class Prechecker(object):
    """Base class for implementing Prechecker
    All the args passed can be accessed via self.args
    """

    def __init__(self, args):
        if args:
            self.args = self.parse_args(list(
                shlex.split(args)
            ))
        else:
            self.args = self.parse_args([])

    def run(self, host):
        """This contains the main logic of the precheck"""
        raise NotImplementedError("implemented in subclass")

    def success(self, host):
        """This contains the main logic incase precheck is successful"""
        raise NotImplementedError("implemented in subclass")

    def failure(self, host):
        """This contains the main logic incase the precheck fails"""
        raise NotImplementedError("implemented in subclass")
