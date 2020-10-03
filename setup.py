from setuptools import setup, find_packages
from collections import defaultdict
from pathlib import Path
import os

setup_args = dict(
    version='3.0.0.dev',
    name='varbio',
    description='Helper library with common code of coexpnetviz and morph',
    long_description=Path('README.rst').read_text(),
    url='https://gitlab.psb.ugent.be/deep_genome/varbio',
    author='Tim Diels',
    author_email='tim@diels.me',
    license='LGPL3',
    packages=find_packages(),
    install_requires=[
        'attrs',
        'chardet',
        'humanize',
        'numpy>=1',
        'pandas',
        'pyyaml',
        'scipy',
    ],
    extras_require={
        'dev': [
            'numpydoc',
            'pylint',
            'pytest>=3',
            'pytest-env',
            'pytest-mock',
            'pytil[data_frame,pkg_resources]==8.*',
            'sphinx>=1',
            'sphinx-rtd-theme',
        ],
    },
    # https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved',
        'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
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
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
)

# Generate package data
#
# Anything placed underneath a directory named 'data' in a package, is added to
# the package_data of that package; i.e. included in the sdist/bdist and
# accessible via pkg_resources.resource_*
project_root = Path(__file__).parent
package_data = defaultdict(list)
for package in setup_args['packages']:
    package_dir = project_root / package.replace('.', '/')
    data_dir = package_dir / 'data'
    if data_dir.exists() and not (data_dir / '__init__.py').exists():
        # Found a data dir
        for parent, _, files in os.walk(str(data_dir)):
            package_data[package].extend(str((data_dir / parent / file).relative_to(package_dir)) for file in files)
setup_args['package_data'] = {k: sorted(v) for k,v in package_data.items()}  # sort to avoid unnecessary git diffs

setup(**setup_args)
