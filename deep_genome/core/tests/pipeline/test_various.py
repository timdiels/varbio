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

from deep_genome.core.pipeline import pipeline_cli
from chicken_turtle_util import path as path_
from chicken_turtle_util.exceptions import InvalidOperationError
from pathlib import Path
import subprocess
import asyncio
import pytest
import psutil
import logging
import signal
import os

class TestContextPipeline(object):
    
    '''
    Test context.pipeline
    '''
    
    def test_raise_uninitialised(self, context):
        '''
        When not initialised, raise on usage
        '''
        with pytest.raises(InvalidOperationError) as ex:
            context.pipeline
        assert 'Pipeline not initialised. Call context.initialise_pipeline first.' in str(ex.value)
    
    def test_job_directory(self, context):
        '''
        Test context.pipeline.job_directory (an internal function)
        '''
        jobs_directory = Path('jobs')
        context.initialise_pipeline(jobs_directory)
        assert jobs_directory in context.pipeline.job_directory('type', 1).parents


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
        with pytest.raises(SystemExit) as ex:
            pipeline_cli(raises(), debug=False)
        assert ex.value.code != 0
        
    @pytest.mark.parametrize('debug', (False, True))
    def test_logging(self, debug, capsys):
        '''
        When debug, DEBUG is included in stderr as well
        
        Assumes chicken_turtle_util.logging.configure is used, thus we don't
        test that part.
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
        stderr += 'I: Pipeline: finished\n'
        actual = capsys.readouterr()[1]
        assert actual == stderr, '\n{}\n---\n{}'.format(actual, stderr)
        
    @pytest.mark.parametrize('signal', (signal.SIGTERM, signal.SIGINT, signal.SIGHUP))
    @pytest.mark.asyncio
    async def test_signal(self, temp_dir_cwd, signal):  # temp_dir_cwd as dg-tests-pipeline-cli-forever puts cache in local directory
        '''
        When the pipeline controller receives SIGTERM or SIGINT, cancel task and exit non-zero.
        '''
        process = await asyncio.create_subprocess_exec('dg-tests-pipeline-cli-selfterm', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Wait for pipeline to have started
        assert (await process.stdout.readline()).decode() == 'Forever started\n'  # Note: reading stdout/stderr is fine as long as you don't write to stdin elsewhere
        
        # Send signal
        process.send_signal(signal)
        
        # Wait for pipeline to have stopped
        result = await process.communicate()
        stdout = result[0].decode()
        stderr = result[1].decode()
        
        # Assert
        assert process.returncode != 0
        assert 'I: Pipeline: cancelling, please wait\nI: Pipeline: cancelled' in stderr
        assert 'Forever cancelled' in stdout
        
# dg-tests-pipeline-cli-forever
def selfterm_command():
    '''
    Pipeline whose coroutine kills the pipeline process and sleeps nearly forever
    '''
    async def selfterm():
        try:
            print('Forever started')
            psutil.Process(os.getpid()).terminate()
            await asyncio.sleep(99999)
        except asyncio.CancelledError:
            print('Forever cancelled')
            raise
    pipeline_cli(selfterm(), debug=False)
    