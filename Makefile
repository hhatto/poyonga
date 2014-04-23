.PHONY: test

all:
	python setup.py build

pypireg:
	python setup.py register
	python setup.py sdist upload

test:
	cd test && python test_poyonga.py

check_static:
	pyflakes poyonga/*
	autopep8 -d -r poyonga

clean:
	rm -rf */__pycache__ temp */*.pyc ./build ./dist ./*.egg-info
