# Some simple testing tasks (sorry, UNIX only).

FLAGS?=--maxfail=3
SCALA_VERSION?=2.13
KAFKA_VERSION?=2.8.1
DOCKER_IMAGE=aiolibs/kafka:$(SCALA_VERSION)_$(KAFKA_VERSION)
DIFF_BRANCH=origin/master
FORMATTED_AREAS=\
	aiokafka/codec.py \
	aiokafka/coordinator/ \
	aiokafka/errors.py \
	aiokafka/helpers.py \
	aiokafka/structs.py \
	aiokafka/util.py \
	aiokafka/protocol/ \
	aiokafka/record/ \
	tests/test_codec.py \
	tests/test_helpers.py \
	tests/test_protocol.py \
	tests/test_protocol_object_conversion.py \
	tests/coordinator/ \
	tests/record/

.PHONY: setup
setup:
	pip install -r requirements-dev.txt
	pip install -Ue .

.PHONY: format
format:
	ruff format aiokafka tests setup.py
	ruff check --fix aiokafka tests setup.py

.PHONY: lint
lint:
	ruff format --check aiokafka tests setup.py
	ruff check aiokafka tests setup.py
	mypy --install-types --non-interactive $(FORMATTED_AREAS)

.PHONY: test
test: lint
	pytest -s --show-capture=no --docker-image $(DOCKER_IMAGE) $(FLAGS) tests

.PHONY: vtest
vtest: lint
	pytest -s -v --log-level INFO --docker-image $(DOCKER_IMAGE) $(FLAGS) tests

.PHONY: cov cover coverage
cov cover coverage: lint
	pytest -s --cov aiokafka --cov-report html --docker-image $(DOCKER_IMAGE) $(FLAGS) tests
	@echo "open file://`pwd`/htmlcov/index.html"

.PHONY: ci-test-unit
ci-test-unit:
	pytest -s --log-format="%(asctime)s %(levelname)s %(message)s" --log-level DEBUG --cov aiokafka --cov-report xml --color=yes $(FLAGS) tests

.PHONY: ci-test-all
ci-test-all:
	pytest -s -v --log-format="%(asctime)s %(levelname)s %(message)s" --log-level DEBUG --cov aiokafka --cov-report xml  --color=yes --docker-image $(DOCKER_IMAGE) $(FLAGS) tests

coverage.xml: .coverage
	coverage xml

.PHONY: diff-cov
diff-cov: coverage.xml
	git fetch
	diff-cover coverage.xml --html-report diff-cover.html --compare-branch=$(DIFF_BRANCH)

.PHONY: check-readme
check-readme:
	python setup.py check -rms

.PHONY: clean
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

.PHONY: doc
doc:
	make -C docs html
	@echo "open file://`pwd`/docs/_build/html/index.html"
