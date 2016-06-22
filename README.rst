Deep Genome Core (DG Core) is a bioinformatics library for writing
bioinformatics algorithms.

Features
========
TODO feature overview

parsers (deep_genome.core.parse):
- affymetrix expression matrices
- other expression matrices
- NCBI bulk data files: gene_info

cleaning (deep_genome.core.clean):
- plain_text: fix malformed line-endings in plain text files, presence of nul-characters,
  ...
- remove low variance rows from expression matrix

pipelines (deep_genome.core.pipeline):
- define a pipeline of jobs with dependencies
- run jobs concurrently locally or on a cluster (via DRMAA, e.g. Open Grid Scheduler)
- generate a CLI to run your pipeline or run directly in code
- interrupt and resume jobs later

Links
=====

- `Documentation <http://pythonhosted.org/dg_core/>`_
- `PyPI <https://pypi.python.org/pypi/dg_core/>`_
- `GitLab <https://github.com/timdiels/dg_core/>`_ TODO

Project decisions
=================

TODO this belongs in a separate doc, link to it from links e.g. or rather in Developer doc

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
--------

We decided to use shell commands as the basis for jobs instead of e.g. a
Python function that is pickled and sent to a server to be executed. This
better matches DRMAA, is more flexible and more KISS.  This way you can run
scripts in different venv's, and run non-Python code directly. The DG CLI
utilities should make it easy enough to make scripts to run as jobs.

We no longer add a suffix number to ambiguous task names. It is tricky to
ensure the same task is reassigned the same suffix in different contexts (e.g.
if the order in which ambiguous tasks are created is not deterministic).

Comparison to Celery: Celery allows running Python functions concurrently and
using the output of one function as the input to a next function. DG pipeline
allows executing executables concurrently and allows you to specify required
resources such as the number of processors the job requires (via server_args to
some `DRMAAJobServer`\ s). DG pipeline's results are passed via the filesystem,
each job gets its own working directory in which a job can write its output, or
it can simply write to stdout and stderr.

Job dependencies need to be specified up front, you usually need to refer to
your dependency's job directory anyway, so this shouldn't be too much of a
hassle. We'd like to keep a Job immutable in general, it's just simpler. If you
did allow a mutable dependency set, do you allow removal? At least you would
block changes as soon as the Job has been run (regardless of whether it completed).
