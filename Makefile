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
# merge-master - merge the test branch into the master branch and push
#
# Other:
#
# etags - build an Emacs TAGS file
########################################################################


# figure out what branch we're on currently
BRANCH=$(shell git symbolic-ref --short HEAD)

# activation script for the Python virtual environment
VENV=venv/bin/activate


# run unit tests
test: $(VENV)
	. $(VENV) && python setup.py test

# alias to (re)build the Python virtual environment
build-venv: $(VENV)

# (re)build the virtual environment if it's missing, or whenever setup.py changes
$(VENV): setup.py
	rm -rf venv && python3 -m venv venv && . $(VENV) && python setup.py develop

# close the current issue branch and merge into dev
close-issue:
	git checkout dev && git merge "$(BRANCH)" && git branch -d "$(BRANCH)"

# push the dev branch to origin
push-dev:
	git checkout dev && git push

# merge the dev branch into test and push both to origin
merge-test:
	git checkout test && git merge dev && git push && git checkout dev

# merge the test branch into master and push both to origin
merge-master: merge-test
	git checkout master && git merge test && git push && git checkout dev

# do a cold install in a temporary virtual environment and run unit tests
test-install:
	rm -rf venv-test && python3 -m venv venv-test && . venv-test/bin/activate && python setup.py install && python setup.py test
	rm -rf venv-test # make sure we clean up

# (re)generate emacs TAGS file
etags:
	find hxl tests -name '*.py' -o -name '*.csv' | xargs etags

# end

