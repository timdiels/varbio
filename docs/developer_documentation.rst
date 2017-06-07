Developer documentation
=======================

Documentation for developers/contributors of the project.

Project decisions
-----------------
We use the same decisions and docstring convention as used in `Chicken Turtle
Util <https://github.com/timdiels/chicken_turtle_util/>`_.

DG database is a MySQL database. Though we use sqlalchemy, supporting other
databases would require to check for MySQL specific SQL, any reliance on MySQL
default configuration and DB limits.

`parse` package understands various file formats, but does not use the
database. This offers users some basics without requiring them to set up a
database.  For more advanced use, the parsed data is loaded in the database,
then returned from the database in its structured form with all bells and
whistles.  The idea here is that advanced features require a database to work
memory efficiently. For single-use data you would indeed have to add, use, then
remove (as it's no longer needed) the data from the database.

'Large' (>1M) blobs of data that are always fetched as a whole (e.g. gene
expression data) are stored in regular binary files on the file system where
the OS can cache them.  This is more efficient (See
http://research.microsoft.com/apps/pubs/default.aspx?id=64525), but comes at
the cost of some added complexity when loading the data (you first have to get
the path to the file from the database, then load the file).


Pipeline
^^^^^^^^

We decided to use shell commands as the basis for jobs instead of e.g. a
Python function that is pickled and sent to a server to be executed. This
better matches DRMAA, is more flexible and more KISS.  This way you can run
scripts in different venv's, and run non-Python code directly. The DG CLI
utilities should make it easy enough to make scripts to run as jobs.

We no longer add a suffix number to ambiguous task names. It is tricky to
ensure the same task is reassigned the same suffix in different contexts (e.g.
if the order in which ambiguous tasks are created is not deterministic).

Comparison to Celery: Celery allows running Python functions and using the
output of one function as the input to a next function. It distributes
computation to different nodes.  DG pipeline allows executing Python code and
executables concurrently and allows you to specify required resources such as
the number of processors the job requires (via server_args to some
`DRMAAJobServer`\ s). DG pipeline's results are passed via the filesystem, each
job gets its own working directory in which a job can write its output, or it
can simply write to stdout and stderr. Python code can be run concurrently on a
single node, but not distributed. Jobs can be distributed using a
DRMAAJobServer.

Jobs and their output directories are immutable; this simplifies things without
really getting in the way of ease of use.