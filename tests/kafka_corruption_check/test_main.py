from __future__ import print_function

import mock

from kafka_utils.kafka_corruption_check import main


FILE_LINE = "Dumping /path/to/file"
CORRECT_LINE = "offset: 2413 position: 173 isvalid: true payloadsize: 3 magic: 0 compresscodec: NoCompressionCodec crc: 1879665293"
INVALID_LINE = "offset: 2413 position: 173 isvalid: false payloadsize: 3 magic: 0 compresscodec: NoCompressionCodec crc: 1879665293"


def mock_output(values):
    output = mock.Mock()
    output.readlines.return_value = values
    return output


def test_find_files_cmd_min():
    cmd = main.find_files_cmd("path", 60, None, None)
    assert cmd == 'find "path" -type f -name "*.log" -mmin -60'


def test_find_files_cmd_start():
    cmd = main.find_files_cmd("path", None, "START", None)
    assert cmd == 'find "path" -type f -name "*.log" -newermt "START"'


def test_find_files_cmd_range():
    cmd = main.find_files_cmd("path", None, "START", "END")
    assert cmd == 'find "path" -type f -name "*.log" -newermt "START" \! -newermt "END"'


@mock.patch(
    "kafka_utils.kafka_corruption_check."
    "main.print_line",
)
def test_parse_output_correct(mock_print):
    main.parse_output("HOST", mock_output([CORRECT_LINE]))
    assert not mock_print.called


@mock.patch(
    "kafka_utils.kafka_corruption_check."
    "main.print_line",
)
def test_parse_output_invalid(mock_print):
    main.parse_output("HOST", mock_output([FILE_LINE, INVALID_LINE]))
    mock_print.assert_called_once_with("HOST", "/path/to/file", INVALID_LINE, "ERROR")


@mock.patch(
    "kafka_utils.kafka_corruption_check."
    "main.get_partition_leaders",
    return_value={"t0-0": 1, "t0-1": 2},
)
def test_filter_leader_files(mock_get_partition):
    filtered = main.filter_leader_files(None, [(1, "host1", ["a/kafka-logs/t0-0/0123.log",
                                                             "a/kafka-logs/t2-0/0123.log",
                                                             "a/kafka-logs/t0-1/0123.log"]),
                                               (2, "host2", ["a/kafka-logs/t0-0/0123.log",
                                                             "a/kafka-logs/t0-1/0123.log"])])
    assert filtered == [(1, 'host1', ['a/kafka-logs/t0-0/0123.log',
                                      'a/kafka-logs/t2-0/0123.log']),
                        (2, 'host2', ['a/kafka-logs/t0-1/0123.log'])]
