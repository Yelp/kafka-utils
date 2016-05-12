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
	tox

acceptance: acceptance8 acceptance9

acceptance8:
	tox -e docker_itest_8

acceptance9:
	tox -e docker_itest_9

coverage:
	tox -e coverage

tag:
	git tag v${PACKAGE_VERSION}

docs:
	tox -e docs

.PHONY: all clean test coverage tag docs
