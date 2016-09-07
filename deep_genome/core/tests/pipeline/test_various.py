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
from deep_genome.core import AlgorithmContext
import subprocess
import asyncio
import pytest
import psutil
import os
from click.testing import CliRunner

Context = AlgorithmContext('1.0.0')

class TestPipelineCLI(object):
    
    def test_success(self, cli_test_args, event_loop):
        '''
        When target job succeeds, exit zero and notify user
        '''
        async def succeeds(context):
            pass
        main = pipeline_cli(succeeds, Context)
        result = CliRunner().invoke(main, cli_test_args, catch_exceptions=False)
        assert result.exit_code == 0
         
    def test_fail(self, cli_test_args, event_loop):
        '''
        When target job fails, exit non-zero and notify user
        '''
        async def raises(context):
            raise Exception('error')
        main = pipeline_cli(raises, Context) 
        result = CliRunner().invoke(main, cli_test_args)
        assert result.exit_code != 0
         
    @pytest.mark.asyncio
    async def test_sigterm(self, cli_test_args):
        '''
        When the pipeline controller is signal interrupted, cancel task and exit non-zero.
        '''
        process = await asyncio.create_subprocess_exec('dg-tests-pipeline-cli-selfterm', *cli_test_args, stdout=subprocess.PIPE)
        stdout, _ = await process.communicate()
        assert process.returncode != 0
        assert 'forever cancelled' in stdout.decode('utf-8')
        
# dg-tests-pipeline-cli-forever
def selfterm_command():
    '''
    Pipeline whose coroutine kills the pipeline process and sleeps nearly forever
    '''
    async def selfterm(context):
        try:
            psutil.Process(os.getpid()).terminate()
            await asyncio.sleep(99999)
        except asyncio.CancelledError:
            print('forever cancelled')
            raise
    return pipeline_cli(selfterm, Context)
selfterm_command = selfterm_command()

def test_call_repr():
    @call_repr(exclude_args={})
    def f(context, call_repr_):
        return call_repr_
    assert f(1) == 'deep_genome.core.tests.pipeline.test_various.test_call_repr.<locals>.f()'
    