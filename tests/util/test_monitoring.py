import mock
import pytest
from kafka.common import KafkaUnavailableError
from kafka.common import OffsetFetchResponse
from test_offsets import MyKafkaClient
from test_offsets import TestOffsetsBase

from yelp_kafka_tool.util.error import UnknownPartitions
from yelp_kafka_tool.util.error import UnknownTopic
from yelp_kafka_tool.util.monitoring import ConsumerPartitionOffsets
from yelp_kafka_tool.util.monitoring import get_consumer_offsets_metadata
from yelp_kafka_tool.util.monitoring import offset_distance
from yelp_kafka_tool.util.monitoring import topics_offset_distance


class TestMonitoring(TestOffsetsBase):

    def test_offset_metadata_invalid_arguments(self, kafka_client_mock):
        with pytest.raises(TypeError):
            get_consumer_offsets_metadata(
                kafka_client_mock,
                "this won't even be consulted",
                "this should be a list or dict",
            )

    def test_offset_metadata_unknown_topic(self, kafka_client_mock):
        with pytest.raises(UnknownTopic):
            get_consumer_offsets_metadata(
                kafka_client_mock,
                "this won't even be consulted",
                ["something that doesn't exist"],
            )

    def test_offset_metadata_unknown_topic_no_fail(self, kafka_client_mock):
        actual = get_consumer_offsets_metadata(
            kafka_client_mock,
            "this won't even be consulted",
            ["something that doesn't exist"],
            raise_on_error=False
        )
        assert not actual

    def test_offset_metadata_unknown_partitions(self, kafka_client_mock):
        with pytest.raises(UnknownPartitions):
            get_consumer_offsets_metadata(
                kafka_client_mock,
                self.group,
                {'topic1': [99]},
            )

    def test_offset_metadata_unknown_partitions_no_fail(self, kafka_client_mock):
        actual = get_consumer_offsets_metadata(
            kafka_client_mock,
            self.group,
            {'topic1': [99]},
            raise_on_error=False
        )
        assert not actual

    def test_offset_metadata_invalid_partition_subset(self, kafka_client_mock):
        with pytest.raises(UnknownPartitions):
            get_consumer_offsets_metadata(
                kafka_client_mock,
                self.group,
                {'topic1': [1, 99]},
            )

    def test_offset_metadata_invalid_partition_subset_no_fail(
        self,
        kafka_client_mock
    ):
        # Partition 99 does not exist, so we expect to have
        # offset metadata ONLY for partition 1.
        expected = [
            ConsumerPartitionOffsets('topic1', 1, 20, 30, 5)
        ]

        actual = get_consumer_offsets_metadata(
            kafka_client_mock,
            self.group,
            {'topic1': [1, 99]},
            raise_on_error=False
        )
        assert 'topic1' in actual
        assert actual['topic1'] == expected

    def test_get_metadata_kafka_error(self, kafka_client_mock):
        with mock.patch.object(
            MyKafkaClient,
            'load_metadata_for_topics',
            side_effect=KafkaUnavailableError("Boom!"),
            autospec=True
        ) as mock_func:
            with pytest.raises(KafkaUnavailableError):
                get_consumer_offsets_metadata(
                    kafka_client_mock,
                    self.group,
                    {'topic1': [99]},
                )
            assert mock_func.call_count == 2

    def test_offset_distance_ok(self, kafka_client_mock):
        assert {0: 0, 1: 10, 2: 20} == offset_distance(
            kafka_client_mock,
            self.group,
            'topic1',
        )

    def test_offset_distance_partition_subset(self, kafka_client_mock):
        assert {1: 10, 2: 20} == offset_distance(
            kafka_client_mock,
            self.group,
            'topic1',
            partitions=[1, 2],
        )

    def test_offset_distance_all_partitions(self, kafka_client_mock):
        kafka_client = kafka_client_mock

        implicit = offset_distance(
            kafka_client,
            self.group,
            'topic1',
        )

        explicit = offset_distance(
            kafka_client,
            self.group,
            'topic1',
            partitions=self.high_offsets['topic1'].keys(),
        )

        assert implicit == explicit

    def test_offset_distance_unknown_group(self, kafka_client_mock):
        with mock.patch.object(
            MyKafkaClient,
            'send_offset_fetch_request',
            side_effect=lambda group, payloads, fail_on_error, callback: [
                callback(
                    OffsetFetchResponse(req.topic, req.partition, -1, None, 3)
                )
                for req in payloads
            ]
        ):
            assert self.high_offsets['topic1'] == offset_distance(
                kafka_client_mock,
                'derp',
                'topic1',
            )

    def test_topics_offset_distance(self, kafka_client_mock):
        expected = {
            'topic1': {0: 0, 1: 10, 2: 20},
            'topic2': {0: 35, 1: 50}
        }
        assert expected == topics_offset_distance(
            kafka_client_mock,
            self.group,
            ['topic1', 'topic2'],
        )

    def test_topics_offset_distance_partition_subset(self, kafka_client_mock):
        expected = {
            'topic1': {0: 0, 1: 10},
            'topic2': {1: 50}
        }
        assert expected == topics_offset_distance(
            kafka_client_mock,
            self.group,
            {'topic1': [0, 1], 'topic2': [1]},
        )

    def test_topics_offset_distance_topic_subset(self, kafka_client_mock):
        expected = {
            'topic1': {0: 0, 1: 10},
        }
        assert expected == topics_offset_distance(
            kafka_client_mock,
            self.group,
            {'topic1': [0, 1]},
        )
