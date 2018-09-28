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
import os

from setuptools import find_packages
from setuptools import setup

from kafka_utils import __version__


with open(
    os.path.join(
        os.path.abspath(os.path.dirname(__file__)),
        "README.md"
    )
) as f:
    README = f.read()


setup(
    name="kafka-utils",
    version=__version__,
    author="Distributed Systems Team",
    author_email="team-dist-sys@yelp.com",
    description="Kafka management utils",
    packages=find_packages(exclude=["scripts*", "tests*"]),
    url="https://github.com/Yelp/kafka-utils",
    license="Apache License 2.0",
    long_description=README,
    keywords="apache kafka",
    scripts=[
        "scripts/kafka-consumer-manager",
        "scripts/kafka-cluster-manager",
        "scripts/kafka-rolling-restart",
        "scripts/kafka-utils",
        "scripts/kafka-check",
        "scripts/kafka-corruption-check",
    ],
    install_requires=[
        "kafka-python>=1.3.2,<1.5.0",
        "kazoo>=2.0,<3.0.0",
        "PyYAML<4.0.0",
        "pytz>=2018.4",
        "requests-futures>0.9.0",
        "paramiko<2.5.0",
        "requests<3.0.0",
        "retrying",
        "six>=1.10.0",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Operating System :: POSIX",
        "Operating System :: MacOS :: MacOS X",
    ],
)
