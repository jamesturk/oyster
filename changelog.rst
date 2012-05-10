oyster changelog
================

0.3.3-dev
---------
    * S3 storage backend bugfix
    * improvements to signal script
    * oyster.ext cloudsearch, elasticsearch, and superfastmatch
    * add tox/python setup.py test (thanks Marc Abramowitz!)

0.3.2
-----
**2012-03-29**
    * become much more tolerant of duplicates
    * skip S3 test if not prepared
    * use doc_class AWS_PREFIX and AWS_BUCKET if set
    * add DEFAULT_STORAGE_ENGINE setting

0.3.1
-----
**2012-03-10**
    * add validation of doc_class
    * add ability to do one-time updates
    * change how hooks work
    * introduce concept of scripts
    * call pymongo's end_request in long running threads
    * better error messages for duplicate URLs
    * lots of flake8-inspired fixes
    * S3 backend: add support for AWS_PREFIX

0.3.0
-----
**2012-02-21**
    * switch Connection to Kernel
    * add concept of doc_class
    * make storage pluggable instead of GridFS
        * add S3 backend
        * add Dummy backend
    * delete obsolete ExternalStoreTask
    * addition of onchanged hook
    * allow id to be set manually

0.2.5
-----
**2011-10-06**
    * lots of fixes to web frontend
    * ExternalStoreTask

0.2.0
-----
**2011-09-20**
    * major refactor: oysterd replaced by celery
    * fix retries

0.1.0
-----
**2011-08-05**
    * initial release, basic document tracking
