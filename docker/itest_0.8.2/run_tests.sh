#!/bin/bash

set -e

function do_at_exit {
  exit_status=$?
  rm -rf build/ dist/ kafka_utils.egg-info/
  rm -rf .tox/log .tox/${ITEST_PYTHON_FACTOR}-acceptance
  find . -name '*.pyc' -delete
  find . -name '__pycache__' -print0 | xargs -0 rm -rf
  exit $exit_status
}

# Clean up artifacts from tests
trap do_at_exit EXIT INT TERM

tox -e ${ITEST_PYTHON_FACTOR}-acceptance
