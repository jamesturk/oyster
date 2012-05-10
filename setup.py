#!/usr/bin/env python

import os
from setuptools import setup

# Hack to prevent stupid "TypeError: 'NoneType' object is not callable" error
# in multiprocessing/util.py _exit_function when running `python
# setup.py test` (see
# http://www.eby-sarna.com/pipermail/peak/2010-May/003357.html)
try:
    import multiprocessing
except ImportError:
    pass

long_description = open('README.rst').read()

setup(name="oyster",
      version='0.3.3-dev',
      py_modules=['oyster'],
      author="James Turk",
      author_email='jturk@sunlightfoundation.com',
      license="BSD",
      url="http://github.com/sunlightlabs/oyster/",
      long_description=long_description,
      description="a proactive document cache",
      platforms=["any"],
      classifiers=["Development Status :: 4 - Beta",
                   "Intended Audience :: Developers",
                   "License :: OSI Approved :: BSD License",
                   "Natural Language :: English",
                   "Operating System :: OS Independent",
                   "Programming Language :: Python",
                   ],
      install_requires=["httplib2 >= 0.6.0", "scrapelib >= 0.7.2",
                        "pymongo >= 1.11", "flask", "celery"],
      tests_require=["nose"],
      test_suite='nose.collector',
      entry_points="""
[console_scripts]
scrapeshell = scrapelib:scrapeshell
"""
      )
