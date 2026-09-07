# Some simple testing tasks (sorry, UNIX only).

PYTHON?=python
FLAGS?=--maxfail=3
ISOLATED?=0
ifeq ($(ISOLATED),1)
PYTEST?=$(PYTHON) -I -m pytest --import-mode=importlib $(FLAGS)
else
PYTEST?=$(PYTHON) -m pytest $(FLAGS)
endif

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
	ruff format aiokafka tests
	ruff check --fix aiokafka tests

.PHONY: lint
lint:
	ruff format --check aiokafka tests
	ruff check aiokafka tests
	mypy --install-types --non-interactive $(FORMATTED_AREAS)
	zizmor --pedantic .github/workflows

.PHONY: test
test: lint
	$(PYTEST) -s --show-capture=no --docker-image $(DOCKER_IMAGE) tests

.PHONY: vtest
vtest: lint
	$(PYTEST) -s -v --log-level INFO --docker-image $(DOCKER_IMAGE) tests

.PHONY: cov cover coverage
cov cover coverage: lint
	$(PYTEST) -s --cov aiokafka --cov-report html --docker-image $(DOCKER_IMAGE) tests
	@echo "open file://`pwd`/htmlcov/index.html"

.PHONY: ci-test-unit
ci-test-unit:
	$(PYTEST) -s --log-format="%(asctime)s %(levelname)s %(message)s" --log-level DEBUG --cov aiokafka --cov-report xml --color=yes tests

.PHONY: ci-test-all
ci-test-all:
	$(PYTEST) -s -v --log-format="%(asctime)s %(levelname)s %(message)s" --log-level DEBUG --cov aiokafka --cov-report xml  --color=yes --docker-image $(DOCKER_IMAGE) tests

coverage.xml: .coverage
	coverage xml

.PHONY: diff-cov
diff-cov: coverage.xml
	git fetch
	diff-cover coverage.xml --html-report diff-cover.html --compare-branch=$(DIFF_BRANCH)

.PHONY: check-readme
check-readme:
	python -m build --sdist --wheel
	python -m twine check --strict dist/*

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
