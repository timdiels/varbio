project = dict(
    name='deep-genome-core',
    package_name='deep_genome.core',
    human_friendly_name='Deep Genome Core',
    description='Genome analysis platform',
    author='VIB/BEG/UGent',
    author_email='tidie@psb.vib-ugent.be',
    python_version=(3,5),
    readme_file='README.rst',
    url='https://gitlab.psb.ugent.be/deep_genome/core', # project homepage.
    download_url='https://example.com/TODO/{version}', # Template for url to download source archive from. You can refer to the current version with {version}. You can get one from github or gitlab for example.
    license='LGPL3',

    # What does your project relate to?
    keywords='bioinformatics genome-analysis',
    
    # Package indices to release to using `ct-release`
    # These names refer to those defined in ~/.pypirc.
    # For pypi, see http://peterdowns.com/posts/first-time-with-pypi.html
    # For devpi, see http://doc.devpi.net/latest/userman/devpi_misc.html#using-plain-setup-py-for-uploading
    index_test = 'pypitest',  # Index to use for testing a release, before releasing to `index_production`. `index_test` can be set to None if you have no test index
    index_production = 'pypi',
    
    # https://pypi.python.org/pypi?%3Aaction=list_classifiers
    # Note: you must add ancestors of any applicable classifier too
    classifiers='''
        Development Status :: 2 - Pre-Alpha
        Intended Audience :: Developers
        Intended Audience :: Science/Research
        License :: OSI Approved
        License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)
        Natural Language :: English
        Operating System :: POSIX
        Operating System :: POSIX :: AIX
        Operating System :: POSIX :: BSD
        Operating System :: POSIX :: BSD :: BSD/OS
        Operating System :: POSIX :: BSD :: FreeBSD
        Operating System :: POSIX :: BSD :: NetBSD
        Operating System :: POSIX :: BSD :: OpenBSD
        Operating System :: POSIX :: GNU Hurd
        Operating System :: POSIX :: HP-UX
        Operating System :: POSIX :: IRIX
        Operating System :: POSIX :: Linux
        Operating System :: POSIX :: Other
        Operating System :: POSIX :: SCO
        Operating System :: POSIX :: SunOS/Solaris
        Operating System :: Unix
        Programming Language :: Python
        Programming Language :: Python :: 3
        Programming Language :: Python :: 3 :: Only
        Programming Language :: Python :: 3.2
        Programming Language :: Python :: 3.3
        Programming Language :: Python :: 3.4
        Programming Language :: Python :: 3.5
        Programming Language :: Python :: Implementation
        Programming Language :: Python :: Implementation :: CPython
        Programming Language :: Python :: Implementation :: Stackless
        Topic :: Scientific/Engineering
        Topic :: Scientific/Engineering :: Bio-Informatics
        Topic :: Scientific/Engineering :: Artificial Intelligence
        Topic :: Software Development
        Topic :: Software Development :: Libraries
        Topic :: Software Development :: Libraries :: Python Modules
    ''',
    
    # Auto generate entry points
    entry_points={
        'console_scripts': [
            'dg-tests-run-pipeline = deep_genome.core.tests.test_pipeline:dg_tests_run_pipeline',
        ],
    },

    # Files not to ignore in pre commit checks, despite them not being tracked by
    # git.
    pre_commit_no_ignore = [
        'test.conf',
    ],
)
