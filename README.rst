**DEPRECATED** - this project is abandoned & will not be seeing future updates

======
oyster
======

oyster is a service for tracking regularly-accessed pages, a sort of proactive cache.

Oyster intends to provide a command line client for interacting with the list of tracked documents and web frontend for viewing the status and retrieving data.  Behind the scenes it uses a celery queue to manage the documents it is tasked with keeping up to date.

oyster was created by James Turk for `Sunlight Labs <http://sunlightlabs.com>`_.

Source is available via `GitHub <http://github.com/sunlightlabs/oyster/>`_

Installation
============

oyster is available on PyPI: `oyster <http://pypi.python.org/pypi/oyster>`_.

The recommended way to install oyster is to simply ``pip install oyster``

Requirements
------------

* python 2.7
* mongodb 2.0
* pymongo 2.0
* scrapelib 0.5+

Usage
=====

* Run celeryd with beat ``celeryd -B --config=oyster.celeryconfig``
* Run oyster HTTP portal ``python oyster/web.py``
* Use oyster.client.Client to add new documents & query the store
