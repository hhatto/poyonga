all:
	python setup.py build

pypireg:
	python setup.py register
	python setup.py sdist upload

clean:
	rm -rf temp */*.pyc ./build ./dist ./*.egg-info
