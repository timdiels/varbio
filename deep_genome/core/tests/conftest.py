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
from deep_genome.core import Context, patch
from deep_genome.core.database import Credentials
import logging
import pytest
import asyncio

# http://stackoverflow.com/a/30091579/1031434
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL) # Ignore SIGPIPE

patch()

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
    return config

@pytest.fixture(scope='session')
def database_credentials(test_conf):
    '''
    Arguments to prepend to any DG CLI invocation
    '''
    return Credentials(**test_conf['database'])

def _create_context(database_credentials):
    return Context(
        database_credentials=database_credentials
    )
    
@pytest.yield_fixture
def context(event_loop, database_credentials, temp_dir_cwd):
    # Note: event_loop: when using initialise_pipeline, test event_loop needs already be set
    context = _create_context(database_credentials)
    yield context
    context.dispose()

@pytest.yield_fixture
def context2(database_credentials, temp_dir_cwd):
    context = _create_context(database_credentials)
    yield context
    context.dispose()

@pytest.fixture
def db(context):
    db = context.database
    db.clear()
    db.create()
    return db

@pytest.yield_fixture
def session(db):
    with db.scoped_session() as session:
        yield session
        
        # No temp stuff left behind
        assert session.sa_session.query(db.e.GeneNameQuery).count() == 0
        assert session.sa_session.query(db.e.GeneNameQueryItem).count() == 0
        
@pytest.yield_fixture
def event_loop(event_loop):
    # Restore old pytest-asyncio behaviour, wouldn't recommend doing this in new projects
    original_loop = asyncio.get_event_loop()
    asyncio.set_event_loop(event_loop)
    yield event_loop
    if not original_loop.is_closed():
        asyncio.set_event_loop(original_loop)
    