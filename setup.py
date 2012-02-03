#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
import poyonga

setup(
    name='poyonga',
    version=poyonga.__version__,
    description="Python Groonga Client",
    long_description=open("README.rst").read(),
    license='MIT License',
    author='Hideo Hattori',
    author_email='hhatto.jp@gmail.com',
    #url='https://github.com/hhatto/pygroonga',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
    ],
    keywords="groonga http",
    packages=['poyonga'],
    zip_safe=False,
)
