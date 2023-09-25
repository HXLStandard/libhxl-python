########################################################################
# Makefile to automate common tasks
#
# build-venv - (re)build the Python virtual environment if needed
# test - run all unit tests
# test-failed - rerun only failed unit tests
# test-install - test a fresh installation in a temporary venv
# publish-pypi - publish a new release to PyPi
# etags - build an Emacs TAGS file
# api-docs - generate HTML documentation from inline comments
# browse-docs - open API docs in the browser
########################################################################


# activation script for the Python virtual environment
VENV=venv/bin/activate

# run unit tests
test: $(VENV)
	. $(VENV) && pytest

# run unit tests
test-failed: $(VENV)
	. $(VENV) && pytest --lf

# alias to (re)build the Python virtual environment
build-venv: $(VENV)

# (re)build the virtual environment if it's missing, or whenever setup.py changes
$(VENV): setup.py requirements.txt
	rm -rf venv
	python3 -m venv venv
	. $(VENV) \
	  && pip3 install -r requirements.txt \
	  && python setup.py develop \
	  && pip install pdoc3

# do a cold install in a temporary virtual environment and run unit tests
test-install: 
	rm -rf venv-test
	python3 -m venv venv-test
	. venv-test/bin/activate \
	  && python setup.py install \
	  && pytest
	rm -rf venv-test # make sure we clean up

# publish a new release on PyPi
publish-pypi: $(VENV)
	rm -rf dist/*
	git checkout upstream/prod
	. $(VENV) \
	  && pip install twine \
	  && python setup.py sdist && twine upload dist/*

# generate API documentation
api-docs: $(VENV)
	rm -rf docs/*
	. $(VENV) \
	  && pdoc3 -o docs/ --html hxl && mv docs/hxl/* docs/
	rmdir docs/hxl/

# browse the API docs
browse-docs:
	firefox docs/index.html

# (re)generate emacs TAGS file
etags:
	find hxl tests -name '*.py' -o -name '*.csv' \
	  | xargs etags

# end

