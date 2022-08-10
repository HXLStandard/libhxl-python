########################################################################
# Makefile to automate common tasks
#
# Test-related targets:
#
# (These require the Python3 venv module)
#
# build-venv - (re)build the Python virtual environment if needed
# test - run the unit tests (building a virtual environment if needed)
# test-install - test a fresh installation in a temporary virtual environment
#
# Git-related targets:
#
# (All of these check out the dev branch at the end)
#
# close-issue - merge the current issue branch into dev and delete
# push-dev - push current dev branch to upstream
# merge-test - merge the dev branch into the test branch and push
# merge-main - merge the test branch into the main branch and push
#
# Other:
#
# etags - build an Emacs TAGS file
########################################################################


# figure out what branch we're on currently
BRANCH=$(shell git symbolic-ref --short HEAD)

# activation script for the Python virtual environment
VENV=venv/bin/activate

# temporary directory for RST API docs
TMPRST = /tmp/libhxl-temp-rst/


# run unit tests
test: $(VENV)
	. $(VENV) && python setup.py test

# alias to (re)build the Python virtual environment
build-venv: $(VENV)

# (re)build the virtual environment if it's missing, or whenever setup.py changes
$(VENV): setup.py requirements.txt
	rm -rf venv && python3 -m venv venv && . $(VENV) && pip3 install -r requirements.txt && python setup.py develop && pip install pdoc3

# close the current issue branch and merge into dev
close-issue:
	git checkout dev && git merge -m "Merge to dev" "$(BRANCH)" && git branch -d "$(BRANCH)"

# push the dev branch to origin
push-dev: api-docs
	git checkout dev && git pull && git push

# merge the dev branch into test and push both to origin
merge-test: push-dev
	git checkout test && git pull && git merge -m "Merge to test" dev && git push && git checkout dev

# merge the test branch into main and push both to origin
merge-main: merge-test
	git checkout main && git pull && git merge -m "Merge to main" test && git push && git checkout dev

# do a cold install in a temporary virtual environment and run unit tests
test-install: 
	rm -rf venv-test && python3 -m venv venv-test && . venv-test/bin/activate && python setup.py install && python setup.py test
	rm -rf venv-test # make sure we clean up

# Make sure we're in sync with upstream (merge down rather than up, in case main or test have changed)
sync:
	git checkout main && git pull && git push && git checkout test && git pull && git merge main && git push && git checkout dev && git pull && git merge test && git push

# publish a new release on PyPi
publish-pypi: $(VENV)
	. $(VENV) && pip install twine && rm -rf dist/* && python setup.py sdist && twine upload dist/*

# generate API documentation
api-docs: $(VENV)
	. $(VENV) && rm -rf docs/* && pdoc3 -o docs/ --html hxl && mv docs/hxl/* docs/ && rmdir docs/hxl/

# browse the API docs
browse-docs:
	firefox docs/index.html

# (re)generate emacs TAGS file
etags:
	find hxl tests -name '*.py' -o -name '*.csv' | xargs etags

# end

