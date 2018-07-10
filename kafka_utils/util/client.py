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
import time

from kafka import SimpleClient
from kafka.conn import get_ip_port_afi
from kafka.errors import ConnectionError
from kafka.errors import FailedPayloadsError
from kafka.errors import GroupCoordinatorNotAvailableError
from kafka.errors import GroupLoadInProgressError
from kafka.errors import NotCoordinatorForGroupError
from retrying import retry

from kafka_utils.util.protocol import KafkaToolProtocol

RETRY_ATTEMPTS = 10
WAIT_BEFORE_RETRYING = 2 * 1000
CONSUMER_OFFSET_TOPIC_CREATION_RETRIES = 20
CONSUMER_OFFSET_RETRY_INTERVAL_SEC = 0.5


def _retry_if_kafka_consumer_coordination_error(exception):
    """

    :param exception: Exception to be checked if its of type
        GroupCoordinatorNotAvailableError
    :return: boolean
    """
    return isinstance(exception, (GroupCoordinatorNotAvailableError, NotCoordinatorForGroupError))


class KafkaToolClient(SimpleClient):
    '''
    Extends the KafkaClient class, and includes a method for sending offset
    commit requests to Kafka.
    '''

    @retry(retry_on_exception=_retry_if_kafka_consumer_coordination_error,
           stop_max_attempt_number=RETRY_ATTEMPTS,
           wait_fixed=WAIT_BEFORE_RETRYING)
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

    def send_consumer_metadata_request(self, payloads=[], fail_on_error=True,
                                       callback=None):
        """
        Encode and decode consumer metadata requests.

        Uses KafkaToolProtocol instead of upstream KafkaProtocol with
        custom consumer_metadata encoding and decoding
        """
        encoder = KafkaToolProtocol.encode_consumer_metadata_request
        decoder = KafkaToolProtocol.decode_consumer_metadata_response

        return self._send_broker_unaware_request(payloads, encoder, decoder)

    def _send_consumer_aware_request(self, group, payloads, encoder_fn, decoder_fn):
        """
        Send a list of requests to the consumer coordinator for the group
        specified using the supplied encode/decode functions. As the payloads
        that use consumer-aware requests do not contain the group (e.g.
        OffsetFetchRequest), all payloads must be for a single group.
        Arguments:
        group: the name of the consumer group (str) the payloads are for
        payloads: list of object-like entities with topic (str) and
            partition (int) attributes; payloads with duplicate
            topic+partition are not supported.
        encode_fn: a method to encode the list of payloads to a request body,
            must accept client_id, correlation_id, and payloads as
            keyword arguments
        decode_fn: a method to decode a response body into response objects.
            The response objects must be object-like and have topic
            and partition attributes
        Returns:
        List of response objects in the same order as the supplied payloads
        """
        # encoders / decoders do not maintain ordering currently
        # so we need to keep this so we can rebuild order before returning
        original_ordering = [(p.topic, p.partition) for p in payloads]

        retries = 0
        broker = None
        while not broker:
            try:
                broker = self._get_coordinator_for_group(group)
            except (GroupCoordinatorNotAvailableError, GroupLoadInProgressError) as e:
                if retries == CONSUMER_OFFSET_TOPIC_CREATION_RETRIES:
                    raise e
                time.sleep(CONSUMER_OFFSET_RETRY_INTERVAL_SEC)
            retries += 1

        # Send the list of request payloads and collect the responses and
        # errors
        responses = {}

        def failed_payloads(payloads):
            for payload in payloads:
                topic_partition = (str(payload.topic), payload.partition)
                responses[topic_partition] = FailedPayloadsError(payload)

        host, port, afi = get_ip_port_afi(broker.host)
        try:
            conn = self._get_conn(host, broker.port, afi)
        except ConnectionError:
            failed_payloads(payloads)

        else:
            request = encoder_fn(payloads=payloads)
            # decoder_fn=None signal that the server is expected to not
            # send a response.  This probably only applies to
            # ProduceRequest w/ acks = 0
            future = conn.send(request)

            while not future.is_done:
                for r, f in conn.recv():
                    f.success(r)

            if future.failed():
                failed_payloads(payloads)

            elif not request.expect_response():
                failed_payloads(payloads)

            else:
                for payload_response in decoder_fn(future.value):
                    topic_partition = (str(payload_response.topic),
                                       payload_response.partition)
                    responses[topic_partition] = payload_response

        # Return responses in the same order as provided
        return [responses[tp] for tp in original_ordering]
