PYTHON ?= python
PYDOCTOR ?= pydoctor

TEST_COMMAND = $(PYTHON) setup.py test

all: build

build:
	$(PYTHON) setup.py build_ext -i

develop:
	$(TEST_COMMAND) --quiet --dry-run

check:
	tox

check-with-trial:
	STORM_TEST_RUNNER=trial tox

doc:
	$(PYDOCTOR) --make-html --html-output apidoc --add-package storm

release:
	$(PYTHON) setup.py sdist

clean:
	rm -rf build
	rm -rf build-stamp
	rm -rf dist
	rm -rf storm.egg-info
	rm -rf debian/files
	rm -rf debian/python-storm
	rm -rf debian/python-storm.*
	rm -rf .tox
	rm -rf *.egg
	rm -rf _trial_temp
	find . -name "*.so" -type f -exec rm -f {} \;
	find . -name "*.pyc" -type f -exec rm -f {} \;
	find . -name "*~" -type f -exec rm -f {} \;

.PHONY: all build check clean develop doc release
