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
import logging

log = logging.getLogger(__name__)


def extract_yelp_replication_group(broker):
    """Extract Yelp replication group from a Broker instance.
    The replication group is the habitat of the broker, which in EC2
    corresponds to the Availability Zone of the broker. This information is
    extracted from the hostname of the broker.
    """
    if broker.inactive:
        # Can't extract replication group from inactive brokers because they
        # don't have metadata
        return None
    try:
        hostname = broker.metadata['host']
        if 'localhost' in hostname:
            log.warning(
                "Setting replication-group as localhost for broker %s",
                broker.id,
            )
            rg_name = 'localhost'
        else:
            habitat = hostname.rsplit('-', 1)[1]
            rg_name = habitat.split('.', 1)[0]
    except IndexError:
        error_msg = "Could not parse replication group for broker {id} with" \
            " hostname:{hostname}".format(id=broker.id, hostname=hostname)
        log.exception(error_msg)
        raise ValueError(error_msg)
    return rg_name
