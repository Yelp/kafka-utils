#!/bin/bash

set -e

function do_at_exit {
  exit_status=$?
  rm -rf build/ dist/ kafka_tools.egg-info/
  rm -rf .tox/log .tox/dist .tox/acceptance
  find . -name '*.pyc' -delete
  find . -name '__pycache__' -print0 | xargs -0 rm -rf
  exit $exit_status
}

# Clean up artifacts from tests
trap do_at_exit EXIT INT TERM

tox -e acceptance
