Deep Blue Genome Core (DBG Core) is ... TODO

Features
========
TODO feature overview

Links
=====

- `Documentation <http://pythonhosted.org/dbg_core/>`_
- `PyPI <https://pypi.python.org/pypi/dbg_core/>`_
- `GitLab <https://github.com/timdiels/dbg_core/>`_ TODO

Project decisions
=================

TODO this belongs in a separate doc, link to it from links e.g. or rather in Developer doc

We use the same decisions and docstring convention as used in `Chicken Turtle Util <https://github.com/timdiels/chicken_turtle_util/>`_.

DBG database is a MySQL database. Though we use sqlalchemy, supporting other databases would require
to check for MySQL specific SQL, any reliance on MySQL default configuration and DB limits.

`parse` package understands various file formats, but does not use the database. This offers users some basics without requiring them to set up a database.
For more advanced use, the parsed data is loaded in the database, then returned from the database in its structured form with all bells and whistles. 
The idea here is that advanced features require a database to work memory efficiently. For single-use data you would indeed have to add, use, then remove
(as it's no longer needed) the data from the database.

'Large' (>1M) blobs of data that are always fetched as a whole (e.g. gene expression data) are stored in regular binary files on the file system where the OS can cache them.
This is more efficient (See http://research.microsoft.com/apps/pubs/default.aspx?id=64525), but comes at the cost of some added complexity when loading the data (you first have to get the path to the file from the database, then load the file).
