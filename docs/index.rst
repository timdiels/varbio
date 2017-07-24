Welcome to varbio's documentation!
==================================
varbio (various bioinformatics) is a bioinformatics utility library, not
recommended for public use due to frequent major releases without backports.

While all features are documented (docstrings only) and tested, the API is
changed frequently. When doing so, the `major version <semver_>`_ is bumped
and a changelog is kept to help upgrade. Fixes will not be backported. It is
recommended to pin the major version in your setup.py, e.g. for 1.x.y::

    install_requires = ['varbio==1.*', ...]

Contents:

.. toctree::
   :maxdepth: 2

   api_reference
   file_formats
   developer_documentation
   changelog


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
 
.. _semver: http://semver.org/spec/v2.0.0.html
