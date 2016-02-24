# Some simple testing tasks (sorry, UNIX only).

FLAGS=


flake:
	flake8 aiokafka tests

test: flake
	nosetests -s $(FLAGS) ./tests/

vtest: flake
	nosetests -s -v $(FLAGS) ./tests/

cov cover coverage:
	nosetests -s --with-cover --cover-html --cover-branches $(FLAGS) --cover-package aiokafka ./tests/  --ignore-files test_consumer.py
	@echo "open file://`pwd`/cover/index.html"

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
	rm -rf coverage
	rm -rf build
	rm -rf cover
	rm -rf dist

doc:
	make -C docs html
	@echo "open file://`pwd`/docs/_build/html/index.html"

.PHONY: all flake test vtest cov clean doc
