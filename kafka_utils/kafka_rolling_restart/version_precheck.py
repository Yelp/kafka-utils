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
import argparse
from functools import partial

from fabric.api import execute
from fabric.api import hide
from fabric.api import settings
from fabric.api import sudo
from fabric.api import task

from .precheck import Prechecker
from .precheck import PrecheckFailedException

CHECK_KAFKA_PACKAGE = "dpkg -s {} | grep '^Version: {}'"
KAFKA_UPDATE_CMD = "run-puppet"


class VersionPreCheck(Prechecker):
    """Class to check for kafka version"""

    def parse_args(self, args):
        parser = argparse.ArgumentParser(prog='VersionPrecheck')
        parser.add_argument(
            '--package-name',
            type=str,
            required=True,
            help='Name of the kafka package',
        )
        parser.add_argument(
            '--package-version',
            type=str,
            required=True,
            help='Version of the kafka package',
        )
        return parser.parse_args(args)

    @task
    def execute_package_update(self, cmd):
        """Execute preconditions before restarting broker"""
        print("Attempting to get latest package")
        with hide('output', 'running', 'warnings'), settings(warn_only=True):
            sudo(cmd)

    @task
    def assert_kafka_package_name_version(self, package_name, package_version):
        """Check if package exists with given package name, return true if it exists"""
        if package_name:
            cmd = CHECK_KAFKA_PACKAGE.format(package_name, package_version)
            with hide('output', 'running', 'warnings'), settings(warn_only=True):
                output = sudo(cmd)
                if output.return_code:
                    raise PrecheckFailedException()

    def run(self, host):
        assert_kafka_package_name_version_func = partial(
            self.assert_kafka_package_name_version,
            VersionPreCheck,
            self.args.package_name,
            self.args.package_version,
        )
        execute(assert_kafka_package_name_version_func, hosts=host)

    def success(self, host):
        print("Precheck for package name and version is successful")

    def failure(self, host):
        print("WARN: Precheck failed for package name and package version")
        execute_package_update_func = partial(
            self.execute_package_update,
            VersionPreCheck,
            KAFKA_UPDATE_CMD,
        )
        execute(execute_package_update_func, hosts=host)
