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
from collections import namedtuple
from functools import partial

from fabric.api import execute
from fabric.api import hide
from fabric.api import settings
from fabric.api import sudo
from fabric.api import task


'''
tuple carrying all the precheck information, which needs to be ensured
before rolling restarting the cluster
'''
prechecks = namedtuple('prechecks', [
    'package_name',
    'package_version',
    'install_cmd',
    'configs',
    'config_file',
])


CHECK_KAFKA_PACKAGE = "dpkg -s {} | grep '^Version: {}'"


class PrecheckFailedException(Exception):
    pass


@task
def execute_package_update(cmd):
    """Execute preconditions before restarting broker"""
    print("Attempting to get latest package")
    with hide('output', 'running', 'warnings'), settings(warn_only=True):
        sudo(cmd)


@task
def check_kafka_package_name_version(package_name, package_version):
    """Check if package exists with given package name, return true if it exists"""
    if package_name:
        cmd = CHECK_KAFKA_PACKAGE.format(package_name, package_version)
        with hide('output', 'running', 'warnings'), settings(warn_only=True):
            output = sudo(cmd)
            if output.return_code:
                print("WARN: expected package in not present on the broker")
                return False
    return True


@task
def check_configs_present(configs, config_file):
    '''Check if the configs are present on the host'''
    with hide('output', 'running', 'warnings'):
        cmd = 'cat ' + config_file
        output = sudo(cmd)
        config_on_host = set(output.split('\r\n'))
        config_present = configs.issubset(config_on_host)
        if not config_present:
            print("WARN: configs are not present on the broker: ", configs)
            return False
        return True


def check_preconditions(package_name, package_version, configs, config_file, host):
    '''Checks whether the preconditions of specific kafka package/version
    are valid for the host'''
    are_preconditions_met = True
    if package_name and package_version:
        check_package_version_func = partial(
            check_kafka_package_name_version,
            package_name,
            package_version,
        )
        are_preconditions_met = are_preconditions_met and \
            all(execute(check_package_version_func, hosts=host).values())
    if configs:
        check_configs_present_func = partial(
            check_configs_present,
            configs,
            config_file,
        )
        are_preconditions_met = are_preconditions_met and \
            all(execute(check_configs_present_func, hosts=host).values())
    return are_preconditions_met


def ensure_preconditions_before_executing_restart(prechecks, host):
    '''Execute a set of prechecks to ensure the required kafka package
    exists on the machine. Incase the required package is not present, schedule a run
    of the configuration management agent, to get the packages installed, and execute
    a post check. Incase package is still not available, this raises an exception
    '''
    preconditions = check_preconditions(
        prechecks.package_name,
        prechecks.package_version,
        prechecks.configs,
        prechecks.config_file,
        host
    )
    if not preconditions:
        # execute a package update
        execute_package_update_func = partial(execute_package_update, prechecks.install_cmd)
        execute(execute_package_update_func, hosts=host)

        # checking if those preconditions are now met else exit
        preconditions = check_preconditions(
            prechecks.package_name,
            prechecks.package_version,
            prechecks.configs,
            prechecks.config_file,
            host
        )
        if not preconditions:
            raise PrecheckFailedException()
