# Auto generated by ct-mksetup
# Do not edit this file, edit ./project.py instead

from setuptools import setup
setup(
    **{   'author': 'VIB/BEG/UGent',
    'author_email': 'timdiels.m@gmail.com',
    'classifiers': [   'Development Status :: 2 - Pre-Alpha',
                       'Intended Audience :: Developers',
                       'Intended Audience :: Science/Research',
                       'License :: OSI Approved',
                       'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
                       'Natural Language :: English',
                       'Operating System :: POSIX',
                       'Operating System :: POSIX :: AIX',
                       'Operating System :: POSIX :: BSD',
                       'Operating System :: POSIX :: BSD :: BSD/OS',
                       'Operating System :: POSIX :: BSD :: FreeBSD',
                       'Operating System :: POSIX :: BSD :: NetBSD',
                       'Operating System :: POSIX :: BSD :: OpenBSD',
                       'Operating System :: POSIX :: GNU Hurd',
                       'Operating System :: POSIX :: HP-UX',
                       'Operating System :: POSIX :: IRIX',
                       'Operating System :: POSIX :: Linux',
                       'Operating System :: POSIX :: Other',
                       'Operating System :: POSIX :: SCO',
                       'Operating System :: POSIX :: SunOS/Solaris',
                       'Operating System :: Unix',
                       'Programming Language :: Python',
                       'Programming Language :: Python :: 3',
                       'Programming Language :: Python :: 3 :: Only',
                       'Programming Language :: Python :: 3.2',
                       'Programming Language :: Python :: 3.3',
                       'Programming Language :: Python :: 3.4',
                       'Programming Language :: Python :: 3.5',
                       'Programming Language :: Python :: Implementation',
                       'Programming Language :: Python :: Implementation :: CPython',
                       'Programming Language :: Python :: Implementation :: Stackless',
                       'Topic :: Scientific/Engineering',
                       'Topic :: Scientific/Engineering :: Artificial Intelligence',
                       'Topic :: Scientific/Engineering :: Bio-Informatics',
                       'Topic :: Software Development',
                       'Topic :: Software Development :: Libraries',
                       'Topic :: Software Development :: Libraries :: Python Modules'],
    'description': 'Genome analysis platform',
    'entry_points': {   'console_scripts': [   'dg-tests-pipeline-cli-selfterm = '
                                               'deep_genome.core.tests.pipeline.test_various:selfterm_command']},
    'extras_require': {   'dev': ['numpydoc', 'sphinx', 'sphinx-rtd-theme'],
                          'test': [   'coverage-pth',
                                      'freezegun>0.3.5',
                                      'networkx',
                                      'pytest',
                                      'pytest-asyncio',
                                      'pytest-benchmark',
                                      'pytest-capturelog',
                                      'pytest-cov',
                                      'pytest-env',
                                      'pytest-mock',
                                      'pytest-timeout',
                                      'pytest-xdist']},
    'install_requires': [   'attrs',
                            'bottleneck',
                            'chicken-turtle-util[path,exceptions,inspect,data_frame,series,test,pymysql,sqlalchemy]',
                            'click',
                            'drmaa',
                            'inflection',
                            'more-itertools',
                            'numexpr',
                            'numpy',
                            'pandas',
                            'plumbum',
                            'psutil',
                            'pymysql',
                            'scikit-learn',
                            'scipy',
                            'sqlalchemy'],
    'keywords': 'bioinformatics genome-analysis',
    'license': 'LGPL3',
    'long_description': 'Deep Genome Core (DG Core) is a bioinformatics library for writing\n'
                        'bioinformatics algorithms.\n'
                        '\n'
                        'Features\n'
                        '========\n'
                        '\n'
                        'TODO feature overview\n'
                        '\n'
                        'parsers (deep\\_genome.core.parse): - affymetrix expression matrices -\n'
                        'other expression matrices - NCBI bulk data files: gene\\_info\n'
                        '\n'
                        'cleaning (deep\\_genome.core.clean): - plain\\_text: fix malformed\n'
                        'line-endings in plain text files, presence of nul-characters, ... -\n'
                        'remove low variance rows from expression matrix\n'
                        '\n'
                        'pipelines (deep\\_genome.core.pipeline): - define a pipeline of jobs with\n'
                        'dependencies - run jobs concurrently locally or on a cluster (via DRMAA,\n'
                        'e.g. Open Grid Scheduler) - generate a CLI to run your pipeline or run\n'
                        'directly in code - interrupt and resume jobs later\n'
                        '\n'
                        'Links\n'
                        '=====\n'
                        '\n'
                        '-  `Documentation <http://pythonhosted.org/dg_core/>`__\n'
                        '-  `PyPI <https://pypi.python.org/pypi/dg_core/>`__\n'
                        '-  `GitLab <https://github.com/timdiels/dg_core/>`__ TODO\n'
                        '\n'
                        'Project decisions\n'
                        '=================\n'
                        '\n'
                        'TODO this belongs in a separate doc, link to it from links e.g. or\n'
                        'rather in Developer doc\n'
                        '\n'
                        'We use the same decisions and docstring convention as used in `Chicken\n'
                        'Turtle Util <https://github.com/timdiels/chicken_turtle_util/>`__.\n'
                        '\n'
                        'DG database is a MySQL database. Though we use sqlalchemy, supporting\n'
                        'other databases would require to check for MySQL specific SQL, any\n'
                        'reliance on MySQL default configuration and DB limits.\n'
                        '\n'
                        'parse package understands various file formats, but does not use the\n'
                        'database. This offers users some basics without requiring them to set up\n'
                        'a database. For more advanced use, the parsed data is loaded in the\n'
                        'database, then returned from the database in its structured form with\n'
                        'all bells and whistles. The idea here is that advanced features require\n'
                        'a database to work memory efficiently. For single-use data you would\n'
                        "indeed have to add, use, then remove (as it's no longer needed) the data\n"
                        'from the database.\n'
                        '\n'
                        "'Large' (>1M) blobs of data that are always fetched as a whole (e.g.\n"
                        'gene expression data) are stored in regular binary files on the file\n'
                        'system where the OS can cache them. This is more efficient (See\n'
                        'http://research.microsoft.com/apps/pubs/default.aspx?id=64525), but\n'
                        'comes at the cost of some added complexity when loading the data (you\n'
                        'first have to get the path to the file from the database, then load the\n'
                        'file).\n'
                        '\n'
                        'Pipeline\n'
                        '--------\n'
                        '\n'
                        'We decided to use shell commands as the basis for jobs instead of e.g. a\n'
                        'Python function that is pickled and sent to a server to be executed.\n'
                        'This better matches DRMAA, is more flexible and more KISS. This way you\n'
                        "can run scripts in different venv's, and run non-Python code directly.\n"
                        'The DG CLI utilities should make it easy enough to make scripts to run\n'
                        'as jobs.\n'
                        '\n'
                        'We no longer add a suffix number to ambiguous task names. It is tricky\n'
                        'to ensure the same task is reassigned the same suffix in different\n'
                        'contexts (e.g. if the order in which ambiguous tasks are created is not\n'
                        'deterministic).\n'
                        '\n'
                        'Comparison to Celery: Celery allows running Python functions and using\n'
                        'the output of one function as the input to a next function. It\n'
                        'distributes computation to different nodes. DG pipeline allows executing\n'
                        'Python code and executables concurrently and allows you to specify\n'
                        'required resources such as the number of processors the job requires\n'
                        "(via server\\_args to some DRMAAJobServers). DG pipeline's results are\n"
                        'passed via the filesystem, each job gets its own working directory in\n'
                        'which a job can write its output, or it can simply write to stdout and\n'
                        'stderr. Python code can be run concurrently on a single node, but not\n'
                        'distributed. Jobs can be distributed using a DRMAAJobServer.\n'
                        '\n'
                        'Jobs and their output directories are immutable; this simplifies things\n'
                        'without really getting in the way of ease of use.\n',
    'name': 'deep-genome-core',
    'package_data': {'deep_genome': ['data/coexpnetviz/README.txt', 'data/coexpnetviz/coexpnetviz_style.xml']},
    'packages': [   'deep_genome',
                    'deep_genome.core',
                    'deep_genome.core.database',
                    'deep_genome.core.pipeline',
                    'deep_genome.core.tests',
                    'deep_genome.core.tests.pipeline',
                    'deep_genome.core.tests.pipeline.local'],
    'url': 'https://gitlab.psb.ugent.be/deep_genome/core',
    'version': '0.0.0'}
)
