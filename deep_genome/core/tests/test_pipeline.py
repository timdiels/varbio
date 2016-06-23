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
Test deep_genome.core.pipeline
'''

from deep_genome.core.pipeline import LocalJobServer, DRMAAJobServer, pipeline_cli, Job, Task
from deep_genome.core import AlgorithmContext
from chicken_turtle_util import path as path_
from chicken_turtle_util.exceptions import InvalidOperationError
from pathlib import Path
import plumbum as pb
from contextlib import contextmanager, suppress
import logging
import asyncio
import pytest
import os
import re
from click.testing import CliRunner

with suppress(RuntimeError):
    import drmaa

@pytest.fixture(autouse=True)
def require_db(db):
    pass

@pytest.fixture
def local_job_server(context):
    return LocalJobServer(context)

@pytest.yield_fixture
def drmaa_job_server(context, test_conf):
    if 'drmaa_jobs_directory' not in test_conf:
        pytest.skip('No DRMAA connection config')
    
    # Get clean jobs dir
    jobs_directory = Path(test_conf['drmaa_jobs_directory'])
    path_.remove(jobs_directory, force=True)
    jobs_directory.mkdir()
    
    #
    server = DRMAAJobServer(context, jobs_directory)
    yield server
    assert_no_live_jobs(server)
    server.dispose()

@pytest.fixture(params=('local_job_server','drmaa_job_server'))
def server(request):
    return request.getfuncargvalue(request.param)
            
def assert_no_live_jobs(server):
    '''
    Assert no jobs are running
    '''
    if isinstance(server, DRMAAJobServer):
        try:
            # this is as close as it gets, there is no way to list all jobs; unless we add something to DRMAA server to keep track of it.
            # if timeout is exceeded, there is definitely something running. We miss the ones that terminate within the next second though.
            server._session.synchronize([drmaa.Session.JOB_IDS_SESSION_ALL], 1, True)
        except drmaa.ExitTimeoutException:
            server._session.control(drmaa.Session.JOB_IDS_SESSION_ALL, drmaa.JobControlAction.TERMINATE)
            assert False, 'Test left jobs running'
    #else LocalJobServer: can't really check

@contextmanager
def assert_task_log(caplog, task, events):
    '''
    Assert log contains task log messages in given order
    '''
    # collect log difference
    original_count = len(caplog.text().splitlines())
    with caplog.atLevel(logging.INFO, logger='deep_genome.core.pipeline'):
        yield
    lines = caplog.text().splitlines()[original_count:]
    
    # assert
    events_seen = []
    for line in lines:
        match = re.search(r"Task (started|failed|finished|cancelled): (.+)", line)
        if match:
            assert match.group(2) == task.name  # event happened on wrong task
            event = match.group(1)
            assert event not in events_seen, 'Event happens twice'
            events_seen.append(event)
    assert events_seen == events

def sh(command):
    return ['sh', '-c', command]

def wait_for_rm(path):
    return 'while [ -e "{}" ]; do sleep 1; done'.format(path)

def ps_aux_contains(term):
    for line in pb.local['ps']('aux').splitlines():
        if term in line:
            return True
    return False

def test_job_interface(local_job_server):
    # The interface for defining jobs
    job1 = Job('job1', local_job_server, ['true'])
    assert job1.name == 'job1'
    
class TestTasks(object):
    
    '''
    Test more advanced properties of Task and its descendants
    '''
    
    class _Task(Task):
        
        def __init__(self, context):
            super().__init__('task1', context)
            self._action = 'succeed'
            
        async def _run(self):
            if self._action == 'succeed':
                pass
            elif self._action == 'fail':
                raise Exception('fail')
            elif self._action == 'forever':
                await asyncio.sleep(9999999)
            else:
                assert False
        
        @property
        def action(self):
            pass
        
        @action.setter
        def action(self, action):
            self._action = action
            
        def assert_not_running(self):
            pass  # we trust asyncio on this one
        
    class _Job(Job):
        
        def __init__(self, server, directory):
            self._inhibitor = directory / 'inhibitor'
            self._file = directory / 'fail'
            self._token = 'jfpw39wuiurjw8w379jfosfus2e7edjf'
            super().__init__(
                'job1', 
                server, 
                [
                    'sh', '-c',
                    '{} ; echo {}; [ ! -e {} ]'
                    .format(wait_for_rm(self._inhibitor), self._token, self._file)
                ]
            )
            self.fail = 'succeed'
            
        @property
        def action(self):
            pass
        
        @action.setter
        def action(self, action):
            if action == 'fail':
                self._file.touch()
            elif action == 'succeed':
                path_.remove(self._file)
                path_.remove(self._inhibitor)
            elif action == 'forever':
                self._inhibitor.touch()
            else:
                assert False
                
        def assert_not_running(self):
            if isinstance(self._server, LocalJobServer):
                assert not ps_aux_contains(self._token)
            else:
                assert_no_live_jobs(self._server)
            
    @pytest.fixture
    def fake_job(self, server):
        fake_job = Job('fake', server, ['true'])
        os.makedirs(str(fake_job.directory), exist_ok=True)
        return fake_job
        
    @pytest.fixture
    def job(self, server, fake_job):
        return self._Job(server, fake_job.directory)
    
    @pytest.fixture
    def _task(self, context):
        return self._Task(context)
    
    @pytest.fixture(params=('job', '_task'))
    def task(self, request, job, _task):
        return request.getfuncargvalue(request.param)
    
    @pytest.mark.asyncio
    async def test_succeed(self, task, caplog):
        # Initially not finished.
        assert not task.finished
        
        # When not finished, multiple invocations of run() return the same
        # asyncio.Task up til the task finishes, fails or is cancelled. It would
        # be fine if it did still return it when finished, it's just not
        # guaranteed.
        assert task.run() == task.run()
        
        # When finish successfully, finished.
        with assert_task_log(caplog, task, ['started', 'finished']):
            await task.run()
        assert task.finished
        
        # When trying to run when already finished, return a noop task
        with assert_task_log(caplog, task, []):
            await task.run()
            await task.run()
                
    @pytest.mark.asyncio
    async def test_fail(self, task, caplog):
        # When task fails, it raises
        # When job exits non-zero, it raises
        task.action = 'fail'
        with assert_task_log(caplog, task, ['started', 'failed']):
            with pytest.raises(Exception):
                await task.run()
        assert not task.finished
        
        # When task recovers, it finishes fine
        task.action = 'succeed'
        with assert_task_log(caplog, task, ['started', 'finished']):
            await task.run()
        assert task.finished
        
    @pytest.mark.asyncio
    async def test_cancel(self, task, caplog):
        # When task cancelled, it raises asyncio.CancelledError
        task.action = 'forever'
        asyncio.get_event_loop().call_later(3, task.cancel)
        with assert_task_log(caplog, task, ['started', 'cancelled']):
            with pytest.raises(asyncio.CancelledError):
                await task.run()
        assert not task.finished
        
        # Check it's not longer running
        task.assert_not_running()
        
        # When task rerun, it finishes fine
        task.action = 'succeed'
        with assert_task_log(caplog, task, ['started', 'finished']):
            await task.run()
        assert task.finished
                    
    @pytest.mark.asyncio
    async def test_persistence_task(self, context, context2):
        '''
        When a task has finished, remember it across runs
        '''
        task = self._Task(context)
        await task.run()
        
        task = self._Task(context2)
        assert task.finished
    
    @pytest.mark.asyncio
    async def test_persistence_job(self, context, context2):
        '''
        When a job has finished, remember it across runs
        '''
        job1 = Job('job1', LocalJobServer(context), ['true'])
        await job1.run()
        
        job1_ = Job('job1', LocalJobServer(context2), ['true'])
        assert job1_.finished
        
    
@pytest.mark.asyncio
async def test_job_success(server, context, test_conf):
    '''
    Test things specific to a job: the success case (nothing specific about the other cases)
    '''
    job1 = Job('job1', server, sh('pwd; echo $$; echo stderr >&2; echo extra; touch file; mkdir dir'))
    
    dir_ = server.get_directory(job1)
    if isinstance(server, LocalJobServer):
        assert dir_ == context.cache_directory / 'jobs' / str(job1.id)
    else:
        assert dir_ == Path(test_conf['drmaa_jobs_directory']) / str(job1.id)
         
    assert job1.directory != server.get_directory(job1)
        
    await job1.run()
    
    # stdout gets dumped in stdout file
    lines = path_.read(job1.stdout_file).splitlines()
    job_cwd = lines[0]
    job_pid = int(lines[1])
    assert Path(job_cwd) == job1.directory
    assert job_pid != os.getpid()  # Job ran in a separate process, such that it cannot affect the pipeline controller process
    
    # stderr gets dumped in stderr file
    assert path_.read(job1.stderr_file) == 'stderr\n'

    # created files in output subdir
    assert (job1.directory / 'file').is_file()
    assert (job1.directory / 'dir').is_dir()
    
    # job data dir and contents are read only
    for dir_, _, files in os.walk(str(server.get_directory(job1))):
        dir_ = Path(dir_)
        assert (dir_.stat().st_mode & 0o777) == 0o500
        for file in files:
            assert ((dir_ / file).stat().st_mode & 0o777) == 0o400
          
class TestDRMAAJobServer(object):
    
    '''
    Tests specific to DRMAAJobServer
    '''
    
    @pytest.mark.asyncio
    async def test_cancel(self, drmaa_job_server, event_loop):
        '''
        When cancel job through other interface (i.e. not DG pipeline), still gracefully raise
        '''
        # Note: we don't actually kill from another process, but DG core will still have no idea, which is the point
        
        # Kill job after 3 sec
        def kill_job():
            event_loop.run_in_executor(None, drmaa_job_server._session.control, drmaa.Session.JOB_IDS_SESSION_ALL, drmaa.JobControlAction.TERMINATE)
        event_loop.call_later(3, kill_job)
        
        # Run job
        job1 = Job('job1', drmaa_job_server, ['sleep', '99999999999'])
        with pytest.raises(Exception):
            await job1.run()

Context = AlgorithmContext('1.0.0')

class TestPipelineCLI(object):
    
    # Note: doesn't matter which job server, so we test with the simplest one to set up
    
    @pytest.fixture
    def fake_job(self, local_job_server):
        fake_job = Job('fake', local_job_server, ['true'])
        os.makedirs(str(fake_job.directory), exist_ok=True)
        return fake_job
    
    @pytest.fixture
    def inhibitor1(self, fake_job):
        '''
        While present, main job will not finish
        '''
        inhibitor1 = fake_job.directory / 'inhibitor1'
        inhibitor1.touch()
        return inhibitor1
    
    @pytest.fixture
    def inhibit_failure(self, fake_job):
        '''
        When removed, main job will fail
        '''
        inhibitor = fake_job.directory / 'inhibit_failure'
        inhibitor.touch()
        return inhibitor
    
    @pytest.fixture
    def main(self, local_job_server, inhibitor1, inhibit_failure, event_loop):
        def create_jobs(context):
            assert isinstance(context, Context)
            return Job('main_job', local_job_server, sh(wait_for_rm(inhibitor1) + '; [ -e {} ]'.format(inhibit_failure)))
        return pipeline_cli(Context, create_jobs)
     
    def test_success(self, main, cli_test_args, inhibitor1):
        '''
        When target job succeeds, exit zero and notify user
        '''
        inhibitor1.unlink()
        result = CliRunner().invoke(main, cli_test_args)
        assert result.exit_code == 0
         
    def test_fail(self, main, cli_test_args, inhibitor1, inhibit_failure):
        '''
        When target job fails, exit non-zero and notify user
        '''
        inhibitor1.unlink()
        inhibit_failure.unlink()
        result = CliRunner().invoke(main, cli_test_args)
        assert result.exit_code != 0
         
    @pytest.mark.asyncio
    async def test_sigterm(self, cli_test_args):
        '''
        When the pipeline controller is signal interrupted, stop any jobs on job
        servers. Exits non-zero.
        '''
        token = dg_tests_run_pipeline_token
        process = await asyncio.create_subprocess_exec('dg-tests-run-pipeline', *cli_test_args)
        
        # wait for job to start
        while not ps_aux_contains(token):
            with suppress(asyncio.TimeoutError):
                assert not await asyncio.wait_for(process.wait(), timeout=.1)  # and also check it's still running
        
        # sigterm and wait for termination
        process.terminate()
        await process.wait()
        
        # assert job is killed
        assert not ps_aux_contains(token)
        
# dg-tests-run-pipeline
def create_jobs(context):
    return Job('deep_genome.core.tests.test_pipeline.main_job', LocalJobServer(context), sh('echo {}; sleep 9999999'.format(dg_tests_run_pipeline_token)))
dg_tests_run_pipeline = pipeline_cli(Context, create_jobs)
dg_tests_run_pipeline_token = 'jif98730rjf9guw80r93uldkfkieosljddakuiuei2oadklkf'

# wishlist (nice to have but don't implement yet)
'''
TODO max concurrently submitted jobs is 30 (make customisable), because "If you plan to submit long jobs (over 60 mins), restrict yourself and submit at most 30 jobs."
Yes, test it (with a smaller max). {job1, job2} s, job1 f, job3 s, ... job2 made to be slower than job1

TODO warn if job takes less than duration (5 min by default)

TODO add job.reset: path_.remove(job.directory) and removes its entry from the
database. We track finishedness in the database, in some table there. Present
in table with job name <=> finished.
+ test
'''
