======
oyster
======

oyster is a service for tracking regularly-accessed pages, a sort of proactive cache.

Oyster intends to provide a command line client for interacting with the list of tracked documents and web frontend for viewing the status and retrieving data.  Behind the scenes it uses a celery queue to manage the documents it is tasked with keeping up to date.

oyster was created by James Turk for `Sunlight Labs <http://sunlightlabs.com>`_.

Source is available via `GitHub <http://github.com/sunlightlabs/oyster/>`_

## ADD PyPI link after release

Installation
============

Requirements
------------

* python 2.7
* mongodb 1.8
* pymongo 1.11
* scrapelib 0.5.5

Usage
=====

* Run celeryd with beat ``celeryd -B --config=oyster.celeryconfig``
* Run oyster HTTP portal ``python oyster/web.py``
* Use oyster.client.Client to add new documents & query the store
