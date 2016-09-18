# Copyright (C) 2016 VIB/BEG/UGent - Tim Diels <timdiels.m@gmail.com>
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

'''
Test deep_genome.core.pipeline._various
'''

from deep_genome.core.pipeline import pipeline_cli, call_repr
from deep_genome.core.pipeline._various import _fully_qualified_name
from chicken_turtle_util import path as path_
from pathlib import Path
import subprocess
import asyncio
import pytest
import psutil
import logging
import os
import re
from textwrap import dedent

class TestPipelineCLI(object):
    
    @pytest.fixture
    def silent_asyncio(self):
        '''
        Silence asyncio
        
        Needs to be done before the event_loop fixture to prevent
        "DEBUG Using selector: EpollSelector"
        '''
        logging.getLogger('asyncio').setLevel(logging.WARNING)
        
    @pytest.yield_fixture(autouse=True)
    def autouse(self, silent_asyncio, event_loop, temp_dir_cwd, caplog):
        # Note: event_loop resets the event loop, used by pipeline_cli
        # Note: temp dir is needed as log file is written to pipeline.conf in cwd
        yield
        
        # Cleanup any handlers added by pipeline_cli
        logger = logging.getLogger()
        for handler in logger.handlers[:]:
            if handler != caplog.handler:
                logger.removeHandler(handler)
                handler.close()
    
    def test_success(self):
        '''
        When target job succeeds, exit zero and notify user
        '''
        async def succeeds():
            pass
        pipeline_cli(succeeds(), debug=False)
         
    def test_fail(self):
        '''
        When target job fails, exit non-zero and notify user
        '''
        async def raises():
            await asyncio.sleep(1)
            raise Exception('error')
        future = raises()
        with pytest.raises(SystemExit) as ex:
            pipeline_cli(future, debug=False)
        assert ex.value.code != 0
        
    @pytest.mark.parametrize('debug', (False, True))
    def test_logging(self, debug, capsys):
        '''
        Test all logging
        '''
        # Run
        async def succeeds():
            logger = logging.getLogger('deep_genome.core.pipeline')
            logger.info('Fake info')
            logger.debug('Fake debug')
        pipeline_cli(succeeds(), debug)
        
        # stderr
        #
        # - level is DEBUG if debug, else INFO
        # - terse log format
        stderr = 'I: Fake info\n'
        if debug:
            stderr += 'D: Fake debug\n'
        actual = capsys.readouterr()[1]
        assert actual == stderr, '\n{}\n---\n{}'.format(actual, stderr)
        
        # log file
        #
        # - regardless of debug mode, level is DEBUG
        # - long format with fairly unambiguous source
        log_file_content = path_.read(Path('pipeline.log'))
        infix = r' [0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3} deep_genome.core.pipeline \(test_various:[0-9]+\):'
        pattern = dedent('''\
            I{0}
            Fake info
             
            D{0}
            Fake debug
            '''
            .format(infix)
        )
        assert re.match(pattern, log_file_content, re.MULTILINE)
         
    @pytest.mark.asyncio
    async def test_sigterm(self, temp_dir_cwd):  # temp_dir_cwd as dg-tests-pipeline-cli-forever puts cache in local directory
        '''
        When the pipeline controller is signal interrupted, cancel task and exit non-zero.
        '''
        process = await asyncio.create_subprocess_exec('dg-tests-pipeline-cli-selfterm', stdout=subprocess.PIPE)
        stdout, _ = await process.communicate()
        assert process.returncode != 0
        assert 'Forever cancelled' in stdout.decode()
        
# dg-tests-pipeline-cli-forever
def selfterm_command():
    '''
    Pipeline whose coroutine kills the pipeline process and sleeps nearly forever
    '''
    async def selfterm():
        try:
            psutil.Process(os.getpid()).terminate()
            await asyncio.sleep(99999)
        except asyncio.CancelledError:
            print('Forever cancelled')
            raise
    pipeline_cli(selfterm(), debug=False)

def test_fully_qualified_name():
    '''
    Assert for top-level and nested function that: 
    
    - it contains the module name,
    - names of nesting scopes leading up to it (e.g. test_fully_qualified_name in the case below),
    - its own name
    '''
    expected = __name__ + '.test_fully_qualified_name'
    assert _fully_qualified_name(test_fully_qualified_name) == expected
    
    def f():
        pass
    assert _fully_qualified_name(f) == expected + '.<locals>.f'
    
def test_call_repr():
    @call_repr()
    def f(a, b=2, *myargs, call_repr_, x=1, **mykwargs):
        return call_repr_
    
    name = _fully_qualified_name(f)
    assert f(1) == name + '(*args=(), a=1, b=2, x=1)'
    assert f(1, 2, 3, x=10, y=20) == name + '(*args=(3,), a=1, b=2, x=10, y=20)'
    
    @call_repr()
    def f2(b, a, call_repr_):
        return call_repr_
    assert f2(1, 2) == _fully_qualified_name(f2) + '(a=2, b=1)'
    
    f3 = call_repr(exclude_args={'a'})(f2.__wrapped__)
    assert f3(1, 2) == _fully_qualified_name(f3) + '(b=1)'
    
    f4 = call_repr(exclude_args={'a', 'b'})(f2.__wrapped__)
    assert f4(1, 2) == _fully_qualified_name(f4) + '()'
    
    @call_repr(exclude_args={})
    def f5(context, call_repr_):
        return call_repr_
    assert f5(1) == _fully_qualified_name(f5) + '()'
    