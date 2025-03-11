PROJECT_NAME = de-project
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PROFILE = default
PIP:=pip


#Create virtual env

environment:
	python -m venv venv

#Install requirements

requirements: environment
	source venv/bin/activate && pip install -r ./requirements.txt

#Format and check

black:
	source venv/bin/activate && black --line-length=79 src/ test/
black-check:
	source venv/bin/activate && black --check --line-length=79 src/ test/
flake8:
	source venv/bin/activate && flake8 src/ test/

format-check: black black-check flake8

#Run security

bandit:
	source venv/bin/activate && bandit -r src/ test/ --skip B101
pip-audit:
	source venv/bin/activate && pip-audit
security: bandit pip-audit

#Run tests

check-coverage:
	source venv/bin/activate && export PYTHONPATH=$(PWD) && pytest --cov=src test/
test:
	source venv/bin/activate && export PYTHONPATH=$(PWD) && pytest

run-tests: check-coverage test