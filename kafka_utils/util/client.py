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

import functools

from kafka import KafkaClient

from kafka_utils.util.protocol import KafkaToolProtocol


class KafkaToolClient(KafkaClient):
    '''
    Extends the KafkaClient class, and includes a method for sending offset
    commit requests to Kafka.
    '''

    def send_offset_commit_request_kafka(
            self, group, payloads=[],
            fail_on_error=True, callback=None):
        encoder = functools.partial(
            KafkaToolProtocol.encode_offset_commit_request_kafka,
            group=group,
        )
        decoder = KafkaToolProtocol.decode_offset_commit_response
        resps = self._send_consumer_aware_request(group, payloads, encoder, decoder)

        return [resp if not callback else callback(resp) for resp in resps
                if not fail_on_error or not self._raise_on_response_error(resp)]
