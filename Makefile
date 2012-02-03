all:
	python setup.py build

clean:
	rm -rf */*.pyc ./build ./dist ./*.egg-info
