from __future__ import print_function

import collections
import inspect
import json
import os
import re
import sys
import time

import numpy
import requests

from kafka_info.utils import config


TIME_CATEGORY = {'sec': 1, 'min': 60, 'hour': 3600, 'day': 86400}

SERIES = {
    # TODO: Not relevant as of now. Will be used as part of KAFKA-243.
    'all_topics_bytes_in_count':
        'sf_metric:kafka.server.BrokerTopicMetrics.BytesInPerSec.count',
    'all_topics_messages_in_count':
        'sf_metric:kafka.server.BrokerTopicMetrics.MessagesInPerSec.count',
    'topic_bytes_in_count':
        'sf_metric:kafka.server.BrokerTopicMetrics.BytesInPerSec.count AND '
        'topic:{topic_id}',
    'topic_messages_in_count':
        'sf_metric:kafka.server.BrokerTopicMetrics.MessagesInPerSec.count AND '
        'topic:{topic_id}',
    'partition_size':
        'kafka.log.Log.Size.value AND topic:{topic_id} AND partition:'
        '{partition_id}'
}

_tmp_auth = None


def send_request(query_url, request_auth=False):
    """Send a get request for `query_url` to the signalfx server.

    Keyword Arguments:
    query_url:   Singafx request query
    request_auth: if True asks for Signalfx token else get signalfx-key from
        conf file. Raises connection-error for invalid signalfx-key.
    """
    global _tmp_auth
    try:
        if request_auth:
            if _tmp_auth is not None:
                signalfx_key = _tmp_auth
            else:
                print("SignalFx Token required")
                print("X-SF-Token: ", end="")
                signalfx_key = sys.stdin.readline()[:-1]
                _tmp_auth = signalfx_key
            header = {"X-SF-Token": signalfx_key}
            req = requests.get(query_url, verify=False, headers=header)
            if req.status_code in [400, 401]:
                print(
                    "[ERROR] Invalid signalfx-key {key}".format(key=signalfx_key),
                    file=sys.stderr
                )
                raise requests.ConnectionError
        else:
            header = {"X-SF-Token": config.conf['signalfx_key']}
            req = requests.get(query_url, verify=False, headers=header)
            if req.status_code in [400, 401]:
                print(
                    "[ERROR] Invalid signalfx-key {key}"
                    .format(key=config.conf['signalfx_key']),
                    file=sys.stderr
                )
                return send_request(query_url, True)
        data = req.text
    except requests.ConnectionError as e:
        raise e
    return data


def warn_none(func):
    def warned_func(*args, **kwargs):
        value = func(*args, **kwargs)
        if value is None:
            (frame, filename, line_number, function_name, lines, index) = \
                inspect.getouterframes(inspect.currentframe())[1]
            print(
                "[WARN] Query returned None in %s (%s:%s)" %
                 (
                     function_name,
                     os.path.basename(filename),
                     line_number
                 ),
                file=sys.stderr
            )
            return 0
        return value
    return warned_func


def get_all_datapoints_signalfx(query, time_from):
    """Send a request to signalfx and returns the json data for all output timeseries

    Keyword Arguments:
    time_from: Relative time for which datapoints will be returned
        (examples: 5min, 6hour, 1day).
    rtype:     json data, representing datapoints (timestamp, value) of all
               valid timeseries for given query
    """
    end_ms = int(round(time.time() * 1000))
    match = re.split(r'(\d+)', time_from)
    resolution = 60000
    try:
        start_ms = end_ms - (int)(match[1]) * TIME_CATEGORY[match[2]] * 1000
    except (IndexError, KeyError) as err:
        print(
            "[ERROR] Invalid given time exception".format(err),
            file=sys.stderr
        )
        sys.exit()
    query_url = "https://api.signalfx.com/v1/timeserieswindow?startMs={0}&"\
                "endMs={1}&resolution={2}&query={3}".format(
                    start_ms, end_ms, resolution, query
                )
    if config.debug:
        print(
            "[ERROR] Signalfx query: {0} {1} {2}".format(
                query,
                time_from),
            file=sys.stderr
        )
    if 'signalfx_key' in config.conf.keys():
        data = send_request(query_url)
    else:
        data = send_request(query_url, True)
    if data == '[]' or data is None:
        return None
    data = json.loads(data)
    if data['errors']:
        print(
            "[WARNING] Received error response {0} for query {1}".format(
                data['errors'],
                query
            )
        )
    if bool(data['data']):
        datapoints = data['data']
    else:
        datapoints = {}
        print(
            "[ERROR] No datapoints found for query {0}".format(query),
            file=sys.stderr
        )
    return datapoints


def eval_data_points(
        data_all_timeseries,
        operation_type='average',
        percentile=95
):
    """Get rate of change for each timeseries and then aggregate the time series.

    Calculate rate-of-change over each timeserie for all given timeseries (data_all_timeseries).
    After that aggregate all timeseries to get aggregated rate-of-change.

    Keyword Arguments:
    data_all_timeseries: Datapoint dictionary for all timeseries
    operation_type:      Type of operation over aggregated timeseries
                         (example: average, percentile)
    percentile:          Value of percentile to be calculated over aggreaged time-series
                         if operation-type is percentile
    return:              (int) Return resultant value after applying given operation-type
                         over aggreated rate-of-change timeserie
    """

    datapoint_net_values = []
    if operation_type in ['average_rate', 'percentile']:
        # Rate of change for each time-serie
        rate_of_ch_all_timeseries = collections.defaultdict(dict)
        for timeserie_key in data_all_timeseries.keys():
            timeserie_data = data_all_timeseries[timeserie_key]
            rate_ch_timeserie = [(
                timeserie_data[i + 1][0],
                max(
                    (timeserie_data[i + 1][1] - timeserie_data[i][1]) /
                    ((timeserie_data[i + 1][0] - timeserie_data[i][0]) / 1000), 0
                )
            ) for i in xrange(0, len(timeserie_data) - 1)]
            rate_of_ch_all_timeseries[timeserie_key] = rate_ch_timeserie
        datapoints_net_rate_map = collections.defaultdict(int)
        for timeserie in rate_of_ch_all_timeseries.values():
            for (timestamp, value) in timeserie:
                datapoints_net_rate_map[timestamp] += value
        datapoint_net_values = datapoints_net_rate_map.values()
    else:
        datapoints_net_value_map = collections.defaultdict(int)
        for timeserie in data_all_timeseries.values():
            for [timestamp, value] in timeserie:
                datapoints_net_value_map[timestamp] += value
        datapoint_net_values = datapoints_net_value_map.values()
    # Aggregate all timeseries
    return {
        'average_rate': sum(datapoint_net_values) /
        float(len(datapoint_net_values)),
        'average': sum(datapoint_net_values) /
        float(len(datapoint_net_values)),
        'percentile': numpy.percentile(
            numpy.array(datapoint_net_values),
            percentile
        )
    }[operation_type]


@warn_none
def evaluate_query(
        query,
        operation_type='sum',
        time_from='30min',
        percentile=95):
    """Evaluate and return value generated by 'query' after applying given operation."""
    data_all_timeseries = get_all_datapoints_signalfx(query, time_from)
    if data_all_timeseries:
        return eval_data_points(data_all_timeseries, operation_type, percentile)
    else:
        return None


def build_query(serie_name, serie_parameters={}):
    """Return signalfx query filter for given serie name and serie-paramters."""
    serie = SERIES[serie_name]
    return(serie.format(**serie_parameters))
