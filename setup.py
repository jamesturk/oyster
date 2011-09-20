#!/usr/bin/env python
from setuptools import setup
from oyster import __version__

long_description = open('README.rst').read()

setup(name="oyster",
      version=__version__,
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
      install_requires=["httplib2 >= 0.6.0", "scrapelib >= 0.5.4",
                        "pymongo >= 1.11", "flask", "celery"],
      entry_points="""
[console_scripts]
scrapeshell = scrapelib:scrapeshell
"""
      )
