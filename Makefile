# Some simple testing tasks (sorry, UNIX only).

FLAGS=
SCALA_VERSION?=2.11
KAFKA_VERSION?=0.9.0.1
DOCKER_IMAGE_NAME=pygo/kafka

flake:
	flake8 aiokafka tests

test: flake
	@py.test -s --no-print-logs --scala-version $(SCALA_VERSION) --kafka-version $(KAFKA_VERSION) --docker-image-name $(DOCKER_IMAGE_NAME) $(FLAGS) tests

vtest: flake
	@py.test -s -v --no-print-logs --scala-version $(SCALA_VERSION) --kafka-version $(KAFKA_VERSION) --docker-image-name $(DOCKER_IMAGE_NAME) $(FLAGS) tests

cov cover coverage:
	@py.test -s --no-print-logs --cov aiokafka --cov-report html --scala-version $(SCALA_VERSION) --kafka-version $(KAFKA_VERSION) --docker-image-name $(DOCKER_IMAGE_NAME) $(FLAGS) tests
	@echo "open file://`pwd`/htmlcov/index.html"

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
	rm -rf build
	rm -rf cover
	rm -rf dist

doc:
	make -C docs html
	@echo "open file://`pwd`/docs/_build/html/index.html"

.PHONY: all flake test vtest cov clean doc
