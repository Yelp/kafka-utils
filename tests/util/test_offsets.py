import copy

import pytest
from kafka.common import NotLeaderForPartitionError
from kafka.common import OffsetCommitResponse
from kafka.common import OffsetFetchResponse
from kafka.common import OffsetResponse
from kafka.common import RequestTimedOutError
from kafka.common import UnknownTopicOrPartitionError

from yelp_kafka_tool.util.offsets import _verify_commit_offsets_requests
from yelp_kafka_tool.util.offsets import advance_consumer_offsets
from yelp_kafka_tool.util.offsets import get_current_consumer_offsets
from yelp_kafka_tool.util.offsets import get_topics_watermarks
from yelp_kafka_tool.util.offsets import OffsetCommitError
from yelp_kafka_tool.util.offsets import PartitionOffsets
from yelp_kafka_tool.util.offsets import rewind_consumer_offsets
from yelp_kafka_tool.util.offsets import set_consumer_offsets
from yelp_kafka_tool.util.offsets import UnknownPartitions
from yelp_kafka_tool.util.offsets import UnknownTopic


@pytest.fixture(params=[['topic1'], set(['topic1']), ('topic1',)])
def topics(request):
    return request.param


class MyKafkaClient(object):

    def __init__(
        self,
        topics,
        group_offsets,
        high_offsets,
        low_offsets
    ):
        self.topics = topics
        self.group_offsets = group_offsets
        self.high_offsets = high_offsets
        self.low_offsets = low_offsets
        self.commit_error = False
        self.offset_request_error = False

    def load_metadata_for_topics(self):
        pass

    def send_offset_request(
        self,
        payloads=None,
        fail_on_error=True,
        callback=None
    ):
        if payloads is None:
            payloads = []

        resps = []
        for req in payloads:
            if req.time == -1:
                offset = self.high_offsets[req.topic].get(req.partition, -1)
            else:
                offset = self.low_offsets[req.topic].get(req.partition, -1)
            if self.offset_request_error:
                error_code = NotLeaderForPartitionError.errno
            elif req.partition not in self.topics[req.topic]:
                error_code = UnknownTopicOrPartitionError.errno
            else:
                error_code = 0
            resps.append(OffsetResponse(
                req.topic,
                req.partition,
                error_code,
                (offset,)
            ))

        return [resp if not callback else callback(resp) for resp in resps]

    def set_commit_error(self):
        self.commit_error = True

    def set_offset_request_error(self):
        self.offset_request_error = True

    def send_offset_commit_request(
        self,
        group,
        payloads=None,
        fail_on_error=True,
        callback=None
    ):
        if payloads is None:
            payloads = []

        resps = []
        for req in payloads:
            if not self.commit_error:
                self.group_offsets[req.topic][req.partition] = req.offset
                resps.append(
                    OffsetCommitResponse(
                        req.topic,
                        req.partition,
                        0
                    )
                )
            else:
                resps.append(
                    OffsetCommitResponse(
                        req.topic,
                        req.partition,
                        RequestTimedOutError.errno
                    )
                )

        return [resp if not callback else callback(resp) for resp in resps]

    def has_metadata_for_topic(self, t):
        return t in self.topics

    def get_partition_ids_for_topic(self, topic):
        return self.topics[topic]

    def send_offset_fetch_request(self, group, reqs, fail_on_error, callback):
        return [
            callback(
                OffsetFetchResponse(
                    req.topic,
                    req.partition,
                    self.group_offsets[req.topic].get(req.partition, -1),
                    None,
                    0 if req.partition in self.group_offsets[req.topic] else 3
                ),
            )
            for req in reqs
        ]


class TestOffsetsBase(object):
    topics = {
        'topic1': [0, 1, 2],
        'topic2': [0, 1]
    }
    group = 'group_name'
    high_offsets = {
        'topic1': {
            0: 30,
            1: 30,
            2: 30,
        },
        'topic2': {
            0: 50,
            1: 50
        }
    }
    low_offsets = {
        'topic1': {
            0: 10,
            1: 5,
            2: 3,
        },
        'topic2': {
            0: 5,
            1: 5,
        }
    }
    group_offsets = {
        'topic1': {
            0: 30,
            1: 20,
            2: 10,
        },
        'topic2': {
            0: 15,
        }
    }

    @pytest.fixture
    def kafka_client_mock(self):
        return MyKafkaClient(
            self.topics,
            copy.deepcopy(self.group_offsets),
            self.high_offsets,
            self.low_offsets
        )


class TestOffsets(TestOffsetsBase):

    def test_get_current_consumer_offsets_invalid_arguments(self, kafka_client_mock):
        with pytest.raises(TypeError):
            get_current_consumer_offsets(
                kafka_client_mock,
                "this won't even be consulted",
                "this should be a list or dict",
            )

    def test_get_current_consumer_offsets_unknown_topic(self, kafka_client_mock):
        with pytest.raises(UnknownTopic):
            get_current_consumer_offsets(
                kafka_client_mock,
                "this won't even be consulted",
                ["something that doesn't exist"],
            )

    def test_get_current_consumer_offsets_unknown_topic_no_fail(self, kafka_client_mock):
        actual = get_current_consumer_offsets(
            kafka_client_mock,
            "this won't even be consulted",
            ["something that doesn't exist"],
            raise_on_error=False
        )
        assert not actual

    def test_get_current_consumer_offsets_unknown_partitions(self, kafka_client_mock):
        with pytest.raises(UnknownPartitions):
            get_current_consumer_offsets(
                kafka_client_mock,
                self.group,
                {'topic1': [99]},
            )

    def test_get_current_consumer_offsets_unknown_partitions_no_fail(self, kafka_client_mock):
        actual = get_current_consumer_offsets(
            kafka_client_mock,
            self.group,
            {'topic1': [99]},
            raise_on_error=False
        )
        assert not actual

    def test_get_current_consumer_offsets_invalid_partition_subset(self, kafka_client_mock):
        with pytest.raises(UnknownPartitions):
            get_current_consumer_offsets(
                kafka_client_mock,
                self.group,
                {'topic1': [1, 99]},
            )

    def test_get_current_consumer_offsets_invalid_partition_subset_no_fail(self, kafka_client_mock):
        actual = get_current_consumer_offsets(
            kafka_client_mock,
            self.group,
            {'topic1': [1, 99]},
            raise_on_error=False
        )
        assert actual['topic1'][1] == 20
        # Partition 99 does not exist so it shouldn't be in the result
        assert 99 not in actual['topic1']

    def test_get_current_consumer_offsets(self, topics, kafka_client_mock):
        actual = get_current_consumer_offsets(
            kafka_client_mock,
            self.group,
            topics
        )
        assert actual == {'topic1': {0: 30, 1: 20, 2: 10}}

    def test_get_topics_watermarks_invalid_arguments(self, kafka_client_mock):
        with pytest.raises(TypeError):
            get_topics_watermarks(
                kafka_client_mock,
                "this should be a list or dict",
            )

    def test_get_topics_watermarks_unknown_topic(self, kafka_client_mock):
        with pytest.raises(UnknownTopic):
            get_topics_watermarks(
                kafka_client_mock,
                ["something that doesn't exist"],
            )

    def test_get_topics_watermarks_unknown_topic_no_fail(self, kafka_client_mock):
        actual = get_topics_watermarks(
            kafka_client_mock,
            ["something that doesn't exist"],
            raise_on_error=False,
        )
        assert not actual

    def test_get_topics_watermarks_unknown_partitions(self, kafka_client_mock):
        with pytest.raises(UnknownPartitions):
            get_topics_watermarks(
                kafka_client_mock,
                {'topic1': [99]},
            )

    def test_get_topics_watermarks_unknown_partitions_no_fail(self, kafka_client_mock):
        actual = get_topics_watermarks(
            kafka_client_mock,
            {'topic1': [99]},
            raise_on_error=False,
        )
        assert not actual

    def test_get_topics_watermarks_invalid_partition_subset(self, kafka_client_mock):
        with pytest.raises(UnknownPartitions):
            get_topics_watermarks(
                kafka_client_mock,
                {'topic1': [1, 99]},
            )

    def test_get_topics_watermarks_invalid_partition_subset_no_fail(self, kafka_client_mock):
        actual = get_topics_watermarks(
            kafka_client_mock,
            {'topic1': [1, 99]},
            raise_on_error=False,
        )
        assert actual['topic1'][1] == PartitionOffsets('topic1', 1, 30, 5)
        assert 99 not in actual['topic1']

    def test_get_topics_watermarks(self, topics, kafka_client_mock):
        actual = get_topics_watermarks(
            kafka_client_mock,
            topics,
        )
        assert actual == {'topic1': {
            0: PartitionOffsets('topic1', 0, 30, 10),
            1: PartitionOffsets('topic1', 1, 30, 5),
            2: PartitionOffsets('topic1', 2, 30, 3),
        }}

    def test_get_topics_watermarks_commit_error(self, topics, kafka_client_mock):
        kafka_client_mock.set_offset_request_error()
        actual = get_topics_watermarks(
            kafka_client_mock,
            {'topic1': [0]},
        )
        assert actual == {'topic1': {
            0: PartitionOffsets('topic1', 0, -1, -1),
        }}

    def test__verify_commit_offsets_requests(self, kafka_client_mock):
        new_offsets = {
            'topic1': {
                0: 123,
                1: 456,
            },
            'topic2': {
                0: 12,
            },
        }
        valid_new_offsets = _verify_commit_offsets_requests(
            kafka_client_mock,
            new_offsets,
            True
        )

        assert new_offsets == valid_new_offsets

    def test__verify_commit_offsets_requests_invalid_types_raise_error(
        self,
        kafka_client_mock
    ):
        new_offsets = "my_str"
        with pytest.raises(TypeError):
            _verify_commit_offsets_requests(
                kafka_client_mock,
                new_offsets,
                True
            )

    def test__verify_commit_offsets_requests_invalid_types_no_raise_error(
        self,
        kafka_client_mock
    ):
        new_offsets = {'topic1': 2, 'topic2': 1}
        with pytest.raises(TypeError):
            _verify_commit_offsets_requests(
                kafka_client_mock,
                new_offsets,
                False
            )

    def test__verify_commit_offsets_requests_bad_partitions(
        self,
        kafka_client_mock
    ):
        new_offsets = {
            'topic1': {
                23: 123,
                11: 456,
            },
            'topic2': {
                21: 12,
            },
        }
        with pytest.raises(UnknownPartitions):
            _verify_commit_offsets_requests(
                kafka_client_mock,
                new_offsets,
                True
            )

    def test__verify_commit_offsets_requests_bad_topics(
        self,
        kafka_client_mock
    ):
        new_offsets = {
            'topic32': {
                0: 123,
                1: 456,
            },
            'topic33': {
                0: 12,
            },
        }
        with pytest.raises(UnknownTopic):
            _verify_commit_offsets_requests(
                kafka_client_mock,
                new_offsets,
                True
            )

    def test__verify_commit_offsets_requests_bad_partitions_no_fail(
        self,
        kafka_client_mock
    ):
        new_offsets = {
            'topic1': {
                0: 32,
                23: 123,
                11: 456,
            },
            'topic2': {
                21: 12,
            },
        }
        valid_new_offsets = _verify_commit_offsets_requests(
            kafka_client_mock,
            new_offsets,
            False
        )
        expected_valid_offsets = {
            'topic1': {
                0: 32,
            },
        }

        assert valid_new_offsets == expected_valid_offsets

    def test__verify_commit_offsets_requests_bad_topics_no_fail(
        self,
        kafka_client_mock
    ):
        new_offsets = {
            'topic32': {
                0: 123,
                1: 456,
            },
            'topic33': {
                0: 12,
            },
        }
        valid_new_offsets = _verify_commit_offsets_requests(
            kafka_client_mock,
            new_offsets,
            False
        )
        assert valid_new_offsets == {}

    def test_advance_consumer_offsets(self, kafka_client_mock):
        topics = {
            'topic1': [0, 1, 2],
            'topic2': [0, 1],
        }
        status = advance_consumer_offsets(
            kafka_client_mock,
            "group",
            topics
        )
        assert status == []
        assert kafka_client_mock.group_offsets == self.high_offsets

    def test_advance_consumer_offsets_fail(self, kafka_client_mock):
        kafka_client_mock.set_commit_error()
        topics = {
            'topic1': [0, 1, 2],
            'topic2': [0, 1],
        }
        expected_status = [
            OffsetCommitError("topic1", 0, RequestTimedOutError.message),
            OffsetCommitError("topic1", 1, RequestTimedOutError.message),
            OffsetCommitError("topic1", 2, RequestTimedOutError.message),
            OffsetCommitError("topic2", 0, RequestTimedOutError.message),
            OffsetCommitError("topic2", 1, RequestTimedOutError.message),
        ]

        status = advance_consumer_offsets(
            kafka_client_mock,
            "group",
            topics
        )
        assert len(status) == len(expected_status)
        for expected in expected_status:
            assert any(actual == expected for actual in status)
        assert kafka_client_mock.group_offsets == self.group_offsets

    def test_rewind_consumer_offsets(self, kafka_client_mock):
        topics = {
            'topic1': [0, 1, 2],
            'topic2': [0, 1],
        }
        status = rewind_consumer_offsets(
            kafka_client_mock,
            "group",
            topics
        )
        status == []
        assert kafka_client_mock.group_offsets == self.low_offsets

    def test_rewind_consumer_offsets_fail(self, kafka_client_mock):
        kafka_client_mock.set_commit_error()
        topics = {
            'topic1': [0, 1, 2],
            'topic2': [0, 1],
        }
        expected_status = [
            OffsetCommitError("topic1", 0, RequestTimedOutError.message),
            OffsetCommitError("topic1", 1, RequestTimedOutError.message),
            OffsetCommitError("topic1", 2, RequestTimedOutError.message),
            OffsetCommitError("topic2", 0, RequestTimedOutError.message),
            OffsetCommitError("topic2", 1, RequestTimedOutError.message),
        ]

        status = rewind_consumer_offsets(
            kafka_client_mock,
            "group",
            topics
        )
        assert len(status) == len(expected_status)
        for expected in expected_status:
            assert any(actual == expected for actual in status)
        assert kafka_client_mock.group_offsets == self.group_offsets

    def test_set_consumer_offsets(self, kafka_client_mock):
        new_offsets = {
            'topic1': {
                0: 100,
                1: 200,
            },
            'topic2': {
                0: 150,
                1: 300,
            },
        }

        status = set_consumer_offsets(
            kafka_client_mock,
            "group",
            new_offsets
        )

        expected_offsets = {
            'topic1': {
                0: 100,
                1: 200,
                2: 10,
            },
            'topic2': {
                0: 150,
                1: 300,
            }
        }
        assert status == []
        assert kafka_client_mock.group_offsets == expected_offsets

    def test_set_consumer_offsets_fail(self, kafka_client_mock):
        kafka_client_mock.set_commit_error()
        new_offsets = {
            'topic1': {
                0: 100,
                1: 200,
            },
            'topic2': {
                0: 150,
                1: 300,
            },
        }
        expected_status = [
            OffsetCommitError("topic1", 0, RequestTimedOutError.message),
            OffsetCommitError("topic1", 1, RequestTimedOutError.message),
            OffsetCommitError("topic2", 0, RequestTimedOutError.message),
            OffsetCommitError("topic2", 1, RequestTimedOutError.message),
        ]

        status = set_consumer_offsets(
            kafka_client_mock,
            "group",
            new_offsets,
            raise_on_error=True
        )

        assert len(status) == len(expected_status)
        for expected in expected_status:
            assert any(actual == expected for actual in status)
        assert kafka_client_mock.group_offsets == self.group_offsets
