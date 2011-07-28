======
oyster
======

oyster is a service for tracking regularly-accessed pages, a sort of proactive cache.

It features a daemon, a command line client for interacting with the tracking list, and a web frontend for viewing the status.

oyster was created by James Turk for `Sunlight Labs <http://sunlightlabs.com>`_.

Source is available via `GitHub <http://github.com/sunlightlabs/oyster/>`_

## ADD PyPI link after release

TODO
====

* turn daemon code into a real oysterd
* internal logging
* oyster shell commands
    * status
    * track
* oysterweb
    * dashboard
    * errors
    * document access
* real testing of internals

Installation
============

Requirements
------------

* python 2.7
* mongodb 1.8
* pymongo 1.11
* scrapelib
