# Some simple testing tasks (sorry, UNIX only).

FLAGS=
SCALA_VERSION?=2.12
KAFKA_VERSION?=2.1.0
DOCKER_IMAGE=aiolibs/kafka:$(SCALA_VERSION)_$(KAFKA_VERSION)
DIFF_BRANCH=origin/master

setup:
	pip install -r requirements-dev.txt
	pip install -Ue .

flake:
	extra=$$(python -c "import sys;sys.stdout.write('--exclude tests/test_pep492.py') if sys.version_info[:3] < (3, 5, 0) else sys.stdout.write('')"); \
	flake8 aiokafka tests $$extra

test: flake
	py.test -s --no-print-logs --docker-image $(DOCKER_IMAGE) $(FLAGS) tests

vtest: flake
	py.test -s -v --docker-image $(DOCKER_IMAGE) $(FLAGS) tests

cov cover coverage: flake
	py.test -s --cov aiokafka --cov-report html --docker-image $(DOCKER_IMAGE) $(FLAGS) tests
	@echo "open file://`pwd`/htmlcov/index.html"

ci-test-unit:
	py.test -s --cov aiokafka --cov-report html $(FLAGS) tests

ci-test-all:
	py.test -s --cov aiokafka --cov-report html --docker-image $(DOCKER_IMAGE) $(FLAGS) tests

coverage.xml: .coverage
	coverage xml

diff-cov: coverage.xml
	git fetch
	diff-cover coverage.xml --html-report diff-cover.html --compare-branch=$(DIFF_BRANCH)

check-readme:
	python setup.py check -rms

clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*~' `
	rm -f `find . -type f -name '.*~' `
	rm -f `find . -type f -name '@*' `
	rm -f `find . -type f -name '#*#' `
	rm -f `find . -type f -name '*.orig' `
	rm -f `find . -type f -name '*.rej' `
	rm -f .coverage
	rm -rf htmlcov
	rm -rf docs/_build/
	rm -rf cover
	rm -rf dist
	rm -f aiokafka/record/_crecords/cutil.c
	rm -f aiokafka/record/_crecords/default_records.c
	rm -f aiokafka/record/_crecords/legacy_records.c
	rm -f aiokafka/record/_crecords/memory_records.c
	rm -f aiokafka/record/_crecords/*.html

doc:
	make -C docs html
	@echo "open file://`pwd`/docs/_build/html/index.html"

.PHONY: all flake test vtest cov clean doc
