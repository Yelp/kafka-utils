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

import shlex


class TaskFailedException(Exception):
    pass


class Task(object):
    """Base class for implementing Task
    All the args passed can be accessed via self.args

    :param args: The program arguments
    """

    def __init__(self, args):
        if args:
            self.args = self.parse_args(list(
                shlex.split(args)
            ))
        else:
            self.args = self.parse_args([])

    def parse_args(self, args):
        """Parse args command line arguments.
        :param args: The list of arguments as strings.
        """
        pass

    def run(self, host):
        """This contains the main logic of the task
        Please note an exception from this method will completely stop the restart
        :param host: the host on which precheck is executed on
        :type host: string
        """
        raise NotImplementedError("Implemented in subclass")


class PreStopTask(Task):
    """Class to be used for any pre stop checks"""


class PostStopTask(Task):
    """Class to be used for any post stop checks"""
