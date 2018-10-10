ifeq ($(findstring .yelpcorp.com,$(shell hostname -f)), .yelpcorp.com)
    export PIP_INDEX_URL ?= https://pypi.yelpcorp.com/simple
else
    export PIP_INDEX_URL ?= https://pypi.python.org/simple
endif


PACKAGE_VERSION := $(shell python setup.py --version)

all: test

clean:
	rm -rf build dist *.egg-info/ .tox/ virtualenv_run
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	find . -name '*.deb' -delete
	find . -name '*.changes' -delete
	make -C docs clean

test:
	tox -e py{27,34,35,36}-unittest

acceptance: acceptance8 acceptance9 acceptance10 acceptance11

acceptance8:
	tox -e py{27,34,35,36}-kafka8-dockeritest

acceptance9:
	tox -e py{27,34,35,36}-kafka9-dockeritest

acceptance10:
	tox -e py{27,34,35,36}-kafka10-dockeritest

acceptance11:
	tox -e py{27,34,35,36}-kafka11-dockeritest

coverage:
	tox -e coverage

tag:
	git tag v${PACKAGE_VERSION}

docs:
	tox -e docs

.PHONY: all clean test coverage tag docs
