Deep Genome Core (DG Core) is a bioinformatics library for writing
bioinformatics algorithms, mainly used by the Deep Genome projects.

Links
=====

- `Documentation <http://pythonhosted.org/deep-genome-core/>`_
- `PyPI <https://pypi.python.org/pypi/deep-genome-core/>`_
- `GitLab <https://gitlab.psb.ugent.be/deep_genome/core>`_

API stability
=============
While all features are documented (docstrings only) and tested, the API is
changed frequently.  When doing so, the `major version <semver_>`_ is bumped
and a changelog is kept to help upgrade. Fixes will not be backported. It is
recommended to pin the major version in your setup.py, e.g. for 2.x.y::

    install_requires = ['deep-genome-core>=2.0.0,<3.0.0', ...]

If you see something you like but need long term stability (e.g. if low
maintenance cost is required), request to have it moved to a stable library
(one with fewer major releases) by `opening an issue`_.

.. _opening an issue: https://gitlab.psb.ugent.be/deep_genome/core/issues

Changelog
=========

`Semantic versioning <semver_>`_ is used.

v1.0.0
------
Initial release.

.. _semver: http://semver.org/spec/v2.0.0.html
