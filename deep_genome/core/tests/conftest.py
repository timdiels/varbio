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
from deep_genome.core import AlgorithmContext, initialise
from click.testing import CliRunner
from pathlib import Path
import pytest

# http://stackoverflow.com/a/30091579/1031434
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL) # Ignore SIGPIPE

initialise()

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

def _create_context(cli_test_args):
    _context = []
    
    Context = AlgorithmContext('1.0.0')
    @Context.command()
    def main(context):
        _context.append(context)
    
    CliRunner().invoke(main, cli_test_args, catch_exceptions=False)
    
    return _context[0]

@pytest.fixture
def context(cli_test_args, temp_dir_cwd, mocker):
    #TODO ignore system config files, i.e. provide your own fixed test configuration, e.g. patch it on top of the context
    mocker.patch('xdg.BaseDirectory.xdg_data_home', str(Path('xdg_data_home').absolute()))
    mocker.patch('xdg.BaseDirectory.xdg_cache_home', str(Path('xdg_cache_home').absolute()))
    return _create_context(cli_test_args)

@pytest.fixture
def context2(context, cli_test_args):
    return _create_context(cli_test_args)

@pytest.fixture
def db(context):
    db = context.database
    db.clear()
    db.create()
    return db
