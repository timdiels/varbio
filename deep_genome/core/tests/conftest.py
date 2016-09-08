# Copyright (C) 2015 VIB/BEG/UGent - Tim Diels <timdiels.m@gmail.com>
# 
# This file is part of Deep Genome.
# 
# Deep Genome is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# Deep Genome is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public License
# along with Deep Genome.  If not, see <http://www.gnu.org/licenses/>.

from configparser import ConfigParser
from chicken_turtle_util.test import temp_dir_cwd
from deep_genome.core import Context, initialise
from click.testing import CliRunner
from pathlib import Path
import logging
import pytest

# http://stackoverflow.com/a/30091579/1031434
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL) # Ignore SIGPIPE

initialise()

@pytest.fixture(autouse=True, scope='session')
def common_init():
    # log levels
    logging.getLogger('chicken_turtle_util').setLevel(logging.INFO)

def pytest_runtest_setup(item): #TODO unused? Might be useful someday though
    marker = item.get_marker('skip_unless_current')
    if marker and not item.get_marker('current'):
        pytest.skip(marker.args[0])

@pytest.fixture(scope='session')
def test_conf(pytestconfig):
    config = ConfigParser()
    config.read([str(pytestconfig.rootdir / 'test.conf')])  # machine specific testing conf goes here
    return config['main']

@pytest.fixture(scope='session')
def cli_test_args(test_conf):
    '''
    Arguments to prepend to any DG CLI invocation
    '''
    return test_conf['cli_args'].split()  # Note: offers no support for args with spaces

@pytest.fixture
def Context_(temp_dir_cwd):
    return Context(
        version='1.0.0',
        data_directory=Path('xdg_data_home'),
        cache_directory=Path('xdg_cache_home')
    )
    
def _create_context(cli_test_args, Context_):
    _context = []
    
    @Context_.command()
    def main(context):
        _context.append(context)
    
    CliRunner().invoke(main, cli_test_args, catch_exceptions=False)
    
    return _context[0]

@pytest.fixture
def context(cli_test_args, Context_):
    return _create_context(cli_test_args, Context_)

@pytest.fixture
def context2(cli_test_args, Context_):
    return _create_context(cli_test_args, Context_)

@pytest.fixture
def db(context):
    db = context.database
    db.clear()
    db.create()
    return db
