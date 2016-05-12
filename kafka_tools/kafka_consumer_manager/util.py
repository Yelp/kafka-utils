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

import sys
from collections import defaultdict

from kazoo.exceptions import NodeExistsError


def preprocess_topics(source_groupid, source_topics, dest_groupid, topics_dest_group):
    """Pre-process the topics in source and destination group for duplicates."""
    # Is the new consumer already subscribed to any of these topics?
    common_topics = [topic for topic in topics_dest_group if topic in source_topics]
    if common_topics:
        print(
            "Error: Consumer Group ID: {groupid} is already "
            "subscribed to following topics: {topic}.\nPlease delete this "
            "topics from new group before re-running the "
            "command.".format(
                groupid=dest_groupid,
                topic=', '.join(common_topics),
            ),
            file=sys.stderr,
        )
        sys.exit(1)
    # Let's confirm what the user intends to do.
    if topics_dest_group:
        in_str = (
            "New Consumer Group: {dest_groupid} already "
            "exists.\nTopics subscribed to by the consumer groups are listed "
            "below:\n{source_groupid}: {source_group_topics}\n"
            "{dest_groupid}: {dest_group_topics}\nDo you intend to copy into"
            "existing consumer destination-group? (y/n)".format(
                source_groupid=source_groupid,
                source_group_topics=source_topics,
                dest_groupid=dest_groupid,
                dest_group_topics=topics_dest_group,
            )
        )
        prompt_user_input(in_str)


def create_offsets(zk, consumer_group, offsets):
    """Create path with offset value for each topic-partition of given consumer
    group.

    :param zk: Zookeeper client
    :param consumer_group: Consumer group id for given offsets
    :type consumer_group: int
    :param offsets: Offsets of all topic-partitions
    :type offsets: dict(topic, dict(partition, offset))
    """
    # Create new offsets
    for topic, partition_offsets in offsets.iteritems():
        for partition, offset in partition_offsets.iteritems():
            new_path = "/consumers/{groupid}/offsets/{topic}/{partition}".format(
                groupid=consumer_group,
                topic=topic,
                partition=partition,
            )
            try:
                zk.create(new_path, value=bytes(offset), makepath=True)
            except NodeExistsError:
                print(
                    "Error: Path {path} already exists. Please re-run the "
                    "command.".format(path=new_path),
                    file=sys.stderr,
                )
                raise


def fetch_offsets(zk, consumer_group, topics):
    """Fetch offsets for given topics of given consumer group.

    :param zk: Zookeeper client
    :param consumer_group: Consumer group id for given offsets
    :type consumer_group: int
    :rtype: dict(topic, dict(partition, offset))
    """
    source_offsets = defaultdict(dict)
    for topic, partitions in topics.iteritems():
        for partition in partitions:
            offset, _ = zk.get(
                "/consumers/{groupid}/offsets/{topic}/{partition}".format(
                    groupid=consumer_group,
                    topic=topic,
                    partition=partition,
                )
            )
            source_offsets[topic][partition] = offset
    return source_offsets


def prompt_user_input(in_str):
    while(True):
        answer = raw_input(in_str + ' ')
        if answer == "n" or answer == "no":
            sys.exit(0)
        if answer == "y" or answer == "yes":
            return
