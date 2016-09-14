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

from deep_genome.core.pipeline import pipeline_cli, call_repr, fully_qualified_name
import subprocess
import asyncio
import pytest
import psutil
import os
from click.testing import CliRunner

class TestPipelineCLI(object):
    
    def test_success(self, event_loop):
        '''
        When target job succeeds, exit zero and notify user
        '''
        async def succeeds():
            pass
        main = pipeline_cli(succeeds, version='1.0.0')
        result = CliRunner().invoke(main, catch_exceptions=False)
        assert result.exit_code == 0
         
    def test_fail(self, event_loop):
        '''
        When target job fails, exit non-zero and notify user
        '''
        async def raises():
            raise Exception('error')
        main = pipeline_cli(raises, version='1.0.0')
        result = CliRunner().invoke(main)
        assert result.exit_code != 0
         
    @pytest.mark.asyncio
    async def test_sigterm(self, temp_dir_cwd):  # temp_dir_cwd as dg-tests-pipeline-cli-forever puts cache in local directory
        '''
        When the pipeline controller is signal interrupted, cancel task and exit non-zero.
        '''
        process = await asyncio.create_subprocess_exec('dg-tests-pipeline-cli-selfterm', stdout=subprocess.PIPE)
        stdout, _ = await process.communicate()
        assert process.returncode != 0
        assert 'forever cancelled' in stdout.decode('utf-8')
        
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
            print('forever cancelled')
            raise
    return pipeline_cli(selfterm, version='1.0.0')
selfterm_command = selfterm_command()

def test_fully_qualified_name():
    '''
    Assert for top-level and nested function that: 
    
    - it contains the module name,
    - names of nesting scopes leading up to it (e.g. test_fully_qualified_name in the case below),
    - its own name
    '''
    expected = __name__ + '.test_fully_qualified_name'
    assert fully_qualified_name(test_fully_qualified_name) == expected
    
    def f():
        pass
    assert fully_qualified_name(f) == expected + '.<locals>.f'
    
def test_call_repr():
    @call_repr(exclude_args={})
    def f(context, call_repr_):
        return call_repr_
    assert f(1) == 'deep_genome.core.tests.pipeline.test_various.test_call_repr.<locals>.f()'
    