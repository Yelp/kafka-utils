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
from __future__ import division
from __future__ import print_function

import logging

from kafka_utils.util.validation import assignment_to_plan
_log = logging.getLogger('kafka-cluster-manager')


def display_cluster_topology(cluster_topology):
    print(assignment_to_plan(cluster_topology.assignment))


def display_assignment_changes(plan_details, to_log=True):
    """Display current and proposed changes in
    topic-partition to replica layout over brokers.
    """
    curr_plan_list, new_plan_list, total_changes = plan_details
    action_cnt = '\n[INFO] Total actions required {0}'.format(total_changes)
    _log_or_display(to_log, action_cnt)
    action_cnt = (
        '[INFO] Total actions that will be executed {0}'
        .format(len(new_plan_list))
    )
    _log_or_display(to_log, action_cnt)
    changes = ('[INFO] Proposed Changes in current cluster-layout:\n')
    _log_or_display(to_log, changes)

    tp_str = 'Topic - Partition'
    curr_repl_str = 'Previous-Assignment'
    new_rep_str = 'Proposed-Assignment'
    tp_list = [tp_repl[0] for tp_repl in curr_plan_list]

    # Display heading
    msg = '=' * 80
    _log_or_display(to_log, msg)
    row = (
        '{tp:^30s}: {curr_rep_str:^20s} ==> {new_rep_str:^20s}' .format(
            tp=tp_str,
            curr_rep_str=curr_repl_str,
            new_rep_str=new_rep_str,
        )
    )
    _log_or_display(to_log, row)
    msg = '=' * 80
    _log_or_display(to_log, msg)

    # Display each topic-partition list with changes
    tp_list_sorted = sorted(tp_list, key=lambda tp: (tp[0], tp[1]))
    for tp in tp_list_sorted:
        curr_repl = [
            tp_repl[1] for tp_repl in curr_plan_list if tp_repl[0] == tp
        ][0]
        proposed_repl = [
            tp_repl[1] for tp_repl in new_plan_list if tp_repl[0] == tp
        ][0]
        tp_str = '{topic} - {partition:<2d}'.format(topic=tp[0], partition=tp[1])
        row = (
            '{tp:<30s}: {curr_repl:<20s} ==> {proposed_repl:<20s}'.format(
                tp=tp_str,
                curr_repl=curr_repl,
                proposed_repl=proposed_repl,
            )
        )
        _log_or_display(to_log, row)


def _log_or_display(to_log, msg):
    """Log or display the information."""
    if to_log:
        _log.info(msg)
    else:
        print(msg)
