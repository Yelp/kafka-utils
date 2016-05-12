# -*- coding: utf-8 -*-
# Copyright 2015 Yelp Inc.
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
from setuptools import find_packages
from setuptools import setup

from kafka_tools import __version__


setup(
    name="kafka-tools",
    version=__version__,
    author="Distributed Systems Team",
    author_email="team-dist-sys@yelp.com",
    description="Kafka management tools",
    packages=find_packages(exclude=["scripts", "tests"]),
    scripts=[
        "scripts/kafka-consumer-manager",
        "scripts/kafka-cluster-manager",
        "scripts/kafka-rolling-restart",
    ],
    install_requires=[
        "kazoo>=2.0.post2,<3.0.0",
        "fabric>=1.8.0,<1.11.0",
        "PyYAML<4.0.0",
        "requests-futures>0.9.0",
        "kafka-python<1.0.0",
        "requests<3.0.0",
    ],
)
