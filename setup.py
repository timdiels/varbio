# Auto generated by ct-mksetup
# Do not edit this file, edit ./project.py instead

from setuptools import setup
setup(
    **{   'author': 'VIB/BEG/UGent',
    'author_email': 'tidie@psb.vib-ugent.be',
    'classifiers': [   'Development Status :: 2 - Pre-Alpha',
                       "'Intended Audience :: Science/Research',",
                       'License :: OSI Approved',
                       'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
                       'Natural Language :: English',
                       'Environment :: Console',
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
                       'Topic :: Scientific/Engineering :: Bio-Informatics',
                       'Topic :: Scientific/Engineering :: Artificial Intelligence',
                       'Topic :: Software Development',
                       'Topic :: Software Development :: Libraries',
                       'Topic :: Software Development :: Libraries :: Python Modules'],
    'description': 'Genome analysis platform',
    'entry_points': {'console_scripts': ['dbg-run-job = deep_blue_genome.core.pipeline:job_runner']},
    'extras_require': {   'dev': ['sphinx', 'numpydoc', 'sphinx-rtd-theme'],
                          'test': [   'pytest',
                                      'pytest-env',
                                      'pytest-xdist',
                                      'pytest-cov',
                                      'coverage-pth',
                                      'pytest-benchmark',
                                      'pytest-timeout',
                                      'pytest-mock',
                                      'pytest-asyncio',
                                      'pytest-capturelog',
                                      'freezegun>0.3.5',
                                      'networkx']},
    'install_requires': [   'click',
                            'numpy',
                            'matplotlib',
                            'scipy',
                            'scikit-learn',
                            'pandas',
                            'numexpr',
                            'bottleneck',
                            'plumbum',
                            'inflection',
                            'more-itertools',
                            'memory-profiler',
                            'psutil',
                            'pyxdg',
                            'frozendict',
                            'requests',
                            'sqlalchemy',
                            'pymysql',
                            'sqlparse',
                            'chicken-turtle-util'],
    'keywords': 'bioinformatics genome-analysis',
    'license': 'LGPL3',
    'long_description': 'Deep Blue Genome Core (DBG Core) is a bioinformatics library for writing\n'
                        'bioinformatics algorithms.\n'
                        '\n'
                        'Features\n'
                        '========\n'
                        '\n'
                        'TODO feature overview\n'
                        '\n'
                        'parsers (deep\\_blue\\_genome.core.parse): - affymetrix expression\n'
                        'matrices - other expression matrices - NCBI bulk data files: gene\\_info\n'
                        '\n'
                        'cleaning (deep\\_blue\\_genome.core.clean): - plain\\_text: fix malformed\n'
                        'line-endings in plain text files, presence of nul-characters, ... -\n'
                        'remove low variance rows from expression matrix\n'
                        '\n'
                        'Links\n'
                        '=====\n'
                        '\n'
                        '-  `Documentation <http://pythonhosted.org/dbg_core/>`__\n'
                        '-  `PyPI <https://pypi.python.org/pypi/dbg_core/>`__\n'
                        '-  `GitLab <https://github.com/timdiels/dbg_core/>`__ TODO\n'
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
                        'DBG database is a MySQL database. Though we use sqlalchemy, supporting\n'
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
                        "could run scripts in different venv's, and run non-Python code directly.\n"
                        'The DBG CLI utilities should make it easy enough to make scripts to run\n'
                        'as jobs.\n',
    'name': 'deep-blue-genome-core',
    'package_data': {   'deep_blue_genome': ['data/coexpnetviz/README.txt', 'data/coexpnetviz/coexpnetviz_style.xml'],
                        'deep_blue_genome.core': ['data/core.defaults.conf']},
    'packages': [   'deep_blue_genome',
                    'deep_blue_genome.data_preparation',
                    'deep_blue_genome.util',
                    'deep_blue_genome.core',
                    'deep_blue_genome.core.database',
                    'deep_blue_genome.core.tests'],
    'url': 'https://bitbucket.org/deep_blue_genome/deep_blue_genome',
    'version': '0.0.0'}
)
