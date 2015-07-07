from __future__ import print_function

import json
import sys
from itertools import groupby


def groupsortby(data, key):
    """Sort and group by the same key."""
    return groupby(sorted(data, key=key), key)


def dict_merge(set1, set2):
    """Joins two dictionaries."""
    return dict(set1.items() + set2.items())


def to_h(num, suffix='B'):
    """Converts a byte value in human readable form."""
    if num is None:  # Show None when data is missing
        return "None"
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def print_json(data):
    """Converts `data` into json and prints it to stdout.

    If stdout is a tty it performs a pretty print.
    """
    if sys.stdout.isatty():
        print(json.dumps(data, indent=4, separators=(',', ': ')))
    else:
        print(json.dumps(data))
