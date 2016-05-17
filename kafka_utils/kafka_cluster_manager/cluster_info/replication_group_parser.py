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


class ReplicationGroupParser(object):
    """Base class for replication group parsers"""

    def get_replication_group(self, broker):
        """Implement the logic to extract the replication group id of a broker.

        This method is called with a broker object as argument.

        Example:

        .. code-block:: python

           class MyParser(ReplicationGroupParser):

               def get_replication_group(self, broker):
                   return broker.metadata['host'].split('.', 2)[1]

        :param broker: py:class:`kafka_utils.kafka_cluster_manager.cluster_info.broker.Broker`
        :returns: a string representing the replication group name of the broker
        """
        raise NotImplementedError("Implement in subclass")


class DefaultReplicationGroupParser(ReplicationGroupParser):

    def get_replication_group(self, broker):
        """Default group is None. All brokers are considered in the same group.

        TODO: Support kafka 0.10 and rack tag.
        """
        return None
