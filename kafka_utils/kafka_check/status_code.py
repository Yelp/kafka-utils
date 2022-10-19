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
import sys

from kafka_utils.util import print_json

OK = 0
WARNING = 1
CRITICAL = 2

STATUS_STRING = {
    OK: 'OK',
    WARNING: 'WARNING',
    CRITICAL: 'CRITICAL',
}


def prepare_terminate_message(string):
    return {
        'message': string,
        'raw': string,
    }


def terminate(err_code, msg, json):
    if json:
        output = {
            'status': STATUS_STRING[err_code],
            'data': msg['raw'],
        }
        print_json(output)
    else:
        print('{status}: {msg}'.format(
            status=STATUS_STRING[err_code],
            msg=msg['message'],
        ))
        if 'verbose' in msg:
            print(msg['verbose'])
    sys.exit(err_code)
