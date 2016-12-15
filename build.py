from pybuilder.core import use_plugin, init, Author, task, before, depends
from pybuilder.errors import BuildFailedException

@task
def tmp_stuff(project):
    for name, value in project.properties.items():
        if 'src' in str(value):
            print(name, value)
            
# plugin imports
import re
import os

use_plugin('python.core')
use_plugin('python.install_dependencies')
use_plugin('python.distutils')
use_plugin('python.sphinx')
use_plugin('copy_resources')
use_plugin('filter_resources')
use_plugin('source_distribution')

default_task = ['clean', 'package']

name = 'deep-genome-core'
version = '1.0.0'
summary = 'Genome analysis library'
description = None  # set to None such that it will be overwritten with readme file contents
url = 'https://gitlab.psb.ugent.be/deep_genome/core'  # Project home page
license = 'LGPLv3'
authors = [
    Author('VIB/BEG/UGent', 'info@psb.ugent.be'),  # Note: an email is required
    Author('Tim Diels', 'timdiels.m@gmail.com'),
]

@init()
def plugin_initialize(project, logger):
    # Validate project name: pybuilder_validate_name, _mode=strict|lenient. No off, that's what #use_plugin is for
    if re.search('\s', project.name):
        raise BuildFailedException('Project name may not contain whitespace, use dashes instead')
    if re.search('_', project.name): #TODO also raise on underscores and upper case unless project.set_property('pybuilder_chicken_turtle_name_validation', 'lenient') # default = 'strict'. When lenient, do not even warn; either raise or don't. This also means the name check needs to happen after init, so user can set this property
        raise BuildFailedException('Project name contains underscores, use dashes instead')
    if project.name.lower() != project.name:
        raise BuildFailedException('Project name contains upper case characters, use lower case instead')
    
    # Assert required files exist
    for file in ['LICENSE.txt', 'README.rst']:
        if not os.path.exists(file):
            raise BuildFailedException('Missing required file: {}'.format(file))

##################################################################
# plugin pybuilder_?: on publish, uploads to PyPI. Also adds validation.

import sys

# python: 2.7.9<=version<3 or 3.2<=version for secure setuptools

@init
def release_init(project): #TODO rm prefix
    project.plugin_depends_on('GitPython')
    project.plugin_depends_on('Versio')
     
@before(['publish'])
def release_pre_publish(project, logger):  #TODO rm prefix
    print('pre publish')
    repo = _get_repo()
    
    # If current commit has no tag, fail
    commit = repo.commit()
    for tag in repo.tags:
        if tag.commit == commit:
            break
    else:
        raise BuildFailedException(
            'Current commit has no tag. '
            'To publish, it should have a tag named "v{version}".'
        )
    
    # If tag is not a version tag or is different from project.version, fail
    try:
        if project.version != _version_from_tag(tag):
            raise BuildFailedException(
                'Version tag ({}) of current commit does not equal project.version ({}).'
                .format(tag.name, project.version)
            )
    except ValueError:
        raise BuildFailedException(
            'Current commit has tag ({}). '
            'To publish, it should have a tag named "v{version}".'
            .format(tag.name)
        )
        
    # If version < newest ancestor version, warn
    ancestors = list(repo.commit().iter_parents())
    versions = []
    for tag in repo.tags:
        if tag.commit in ancestors:
            try:
                versions.append(_version_from_tag(tag))
            except ValueError:
                pass
    newest_ancestor_version = max(versions, default=Version('0.0.0'))
             
    if project.version < newest_ancestor_version:
        logger.warn(
            'project.version ({}) is less than that of an ancestor commit ({})'
            .format(project.version, newest_ancestor_version)
        )
    
@task('publish')
@depends('sphinx_generate_documentation')
def release_publish(project):
    print('publish')
    # If setuptools does not use https by default, cop out 
    python_version = (sys.version_info.major, sys.version_info.minor, sys.version_info.patch)
    insecure = (
        python_version < (2, 7, 9) or
        (python_version[0] == 3 and python_version[:2] < (3, 2))
    )
    if insecure:
        raise BuildFailedException(
            'Python version must be >=2.7.9 if Python 2, or >=3.2 if Python 3. '
            'setuptools sends password in plain text in older Python versions. '
            'twine is not used as it does not support uploading documentation, '
            'in which case setuptools is still needed.'
        )
    
    # Ensure the project is registered
    index = project.get_property('distutils_upload_repository')
    _setup('register', '-r', index)
    
    # Upload package
    _setup('sdist', 'bdist_wheel', 'upload', '-r', index)
    
    # Upload docs
    _setup('upload_docs', '-r', index, '--upload-dir', project.expand_path('sphinx_output_dir'))
    
def Version(*args, **kwargs):
    import versio.version
    import versio.version_scheme
    return versio.version.Version(*args, scheme=versio.version_scheme.Pep440VersionScheme, **kwargs)

def _get_repo():
    import git
    return git.Repo('.git')

def _version_from_tag(tag):
    '''
    Get version from version tag
     
    Returns
    -------
    str
        The version the version tag represents 
     
    Raises
    ------
    ValueError
        If tag name is not of format v{version}, i.e. not a version tag
    '''
    name = tag.name.split(os.sep)[-1]
    if not name.startswith('v'):
        raise ValueError('{} is not a version tag'.format(tag))
    return name[1:]

def _setup(*args):
    import plumbum as pb
    command = pb.local['python']['setup.py'].__getitem__(args)
    code, out, err = command.run()  # always has exit code 0
    if 'Server response (200): OK' not in (out + err):
        raise BuildFailedException(
            'Failed to run: {}\n\nstdout:\n{}\n\nstderr:\n{}\n\nexit code: {}\n\n'
            .format(command, out, err, code)
        )

################################
# pytest plugin

import os

# python: 2.6 (because py.test and plumbum)

@init
def pytest_init(project):  #TODO rm prefix
    project.plugin_depends_on('plumbum')
    project.plugin_depends_on('pytest')
    project.set_property_if_unset('dir_source_unittest_python', 'src/unittest/python')
    
@task('run_unit_tests')
def pytest_run_unit_tests(project, logger):  #TODO rm prefix
    print('run_unit_tests')
    return #TODO reenable
    import plumbum as pb
    
    # PYTHONPATH
    path_parts = []
    if 'PYTHONPATH' in pb.local.env:
        path_parts.append(pb.local.env['PYTHONPATH'])
    path_parts.append(project.expand_path('$dir_source_main_python'))
    dir_source_unittest_python = project.expand_path('$dir_source_unittest_python')
    path_parts.append(dir_source_unittest_python)
    PYTHONPATH = os.pathsep.join(path_parts)
    
    # Run
    with pb.local.env(PYTHONPATH=PYTHONPATH):
        try:
            pb.local['py.test'][dir_source_unittest_python] & pb.FG
        except pb.ProcessExecutionError as ex:
            raise BuildFailedException('py.test failed') from ex

    
################################

@init()
def build_dependencies(project):
    # hacks until plugins are separate project
    project.set_property_if_unset('pybuilder_pip_tools_build_urls', [])

    # PyBuilder
    project.build_depends_on('pybuilder')
    project.get_property('pybuilder_pip_tools_build_urls').extend([
        'git+https://github.com/pybuilder/pybuilder.git#egg=pybuilder-0'
    ])
    
    # Testing
    project.build_depends_on('pytest-env')
    project.build_depends_on('pytest-benchmark')
    project.build_depends_on('pytest-timeout')
    project.build_depends_on('pytest-mock')
    project.build_depends_on('pytest-asyncio')
    project.build_depends_on('pytest-capturelog')
    project.build_depends_on('freezegun', '>0.3.5')
    project.build_depends_on('networkx')
    
    # Sphinx doc (sphinx already imported by python.sphinx plugin)
    project.build_depends_on('numpydoc')
    project.build_depends_on('sphinx-rtd-theme')

@init()
def runtime_dependencies(project):
    # hacks until plugins are separate project  
    project.set_property_if_unset('pybuilder_pip_tools_urls', [])
    
    # Required
    project.depends_on('attrs')
    project.depends_on('click')
    project.depends_on('numpy')
    project.depends_on('scipy')
    project.depends_on('scikit-learn')
    project.depends_on('pandas')
    project.depends_on('numexpr')
    project.depends_on('bottleneck')
    project.depends_on('plumbum')
    project.depends_on('inflection')
    project.depends_on('more_itertools')
    project.depends_on('psutil')
    project.depends_on('sqlalchemy')
    project.depends_on('pymysql')
    project.depends_on('drmaa')
    project.depends_on('psutil')

    project.depends_on('chicken-turtle-util[path,exceptions,inspect,data_frame,series,test,pymysql,sqlalchemy]', '>=4.0.0,<5.0.0')
    #/home/limyreth/doc/eclipse_workspace/chicken_turtle_util
    
@init()
def initialize(project):
    # Package data
    # E.g. project.include_file('the.pkg', 'relative/to/pkg/some_file')

    # Files not to include in source distribution 
    project.get_property('source_dist_ignore_patterns').extend([
        '.project',
        '.pydevproject',
        '.settings'
    ])

    # Files to copy to dist root
    project.set_property('copy_resources_target', '$dir_dist')
    project.get_property('copy_resources_glob').extend([
        'README.rst',
        'LICENSE.txt',
    ])

    # Files in which to replace placeholders like ${version}
    project.get_property('filter_resources_glob').extend([
        '**/deep_genome/core/__init__.py',
    ])

    # setup.py
    project.set_property('distutils_readme_description', True)  # set project.description to content of readme file
    project.set_property('distutils_readme_file', 'README.rst')  # readme file name, should be at the root (like build.py). Defaults to README.md
    project.set_property('distutils_console_scripts', [  # entry points
    ])
    project.set_property('distutils_setup_keywords', 'bioinformatics genome-analysis')
    project.set_property('distutils_classifiers', [  # https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 2 - Pre-Alpha',
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
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: Stackless',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ])
    
    # doc: sphinx
    project.set_property('sphinx_project_name', 'Deep Genome Core')
    project.set_property('sphinx_run_apidoc', True)
    project.set_property('sphinx_source_dir', 'src/doc/sphinx')
    project.set_property('sphinx_config_path', 'src/doc/sphinx')
    project.set_property('sphinx_output_dir', '$dir_target/doc/sphinx')

    # package upload
    project.set_property('distutils_upload_repository', 'pypitest')  # index to upload to
