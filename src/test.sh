#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

# Coverage 6: coverage run --data-file=/tmp/.coveragerc …
export COVERAGE_FILE=/tmp/.coverage

echo "Running tests"
coverage run  --omit="gobupload/dev_utils/*" --source=./gobupload -m pytest tests/

echo "Coverage report"
coverage report --show-missing --fail-under=95

echo "Running style checks"
flake8 ./gobupload
