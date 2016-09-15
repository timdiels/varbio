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
Test deep_genome.core.pipeline._job
'''

from .common import assert_task_log
from deep_genome.core.pipeline import LocalJobServer, DRMAAJobServer, Job
from chicken_turtle_util import path as path_
from pathlib import Path
import plumbum as pb
from contextlib import contextmanager, suppress
import os
import asyncio
import pytest

with suppress(RuntimeError):
    import drmaa
            
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
    
def wait_for_rm(path):
    return 'while [ -e "{}" ]; do sleep 1; done'.format(path)

def ps_aux_contains(term):
    for line in pb.local['ps']('aux').splitlines():
        if term in line:
            return True
    return False

@contextmanager
def LocalJobServerFixture(context):
    yield LocalJobServer(context)
    
@contextmanager
def DRMAAJobServerFixture(context, test_conf):
    '''
    Note: can't have multiple active at the same time
    '''
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
    
@pytest.yield_fixture
def drmaa_job_server(context, test_conf):
    with DRMAAJobServerFixture(context, test_conf) as server:
        yield server

@pytest.yield_fixture
def local_job_server(context):
    with LocalJobServerFixture(context) as server:
        yield server

@pytest.yield_fixture(params=('local','drmaa'))
def server(request, context, test_conf):
    '''
    Any server
    '''
    if request.param == 'local':
        server = LocalJobServerFixture(context)
    else:
        server = DRMAAJobServerFixture(context, test_conf)
    with server as server:
        yield server
        
class JobMock(Job):
    
    '''
    Note: As there can be only one active DRMAA job server, there can also
    only be one active JobMock (call dispose to use a new one)
    '''
    
    def __init__(self, server, directory, caplog):
        self._inhibitor = directory / 'inhibitor'
        self._file = directory / 'fail'
        self._token = 'jfpw39wuiurjw8w379jfosfus2e7edjf'
        self._caplog = caplog
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
            
    def assert_log(self, events):
        return assert_task_log(self._caplog, 'Job', 'job1', events)
            
@contextmanager
def JobMockFixture(server_type, context, test_conf, caplog):
    if server_type == 'local':
        server = LocalJobServerFixture(context)
    else:
        server = DRMAAJobServerFixture(context, test_conf)
    with server as server:
        fake_job = Job('fake', server, ['true'])
        os.makedirs(str(fake_job.directory), exist_ok=True)
        directory = fake_job.directory
        path_.remove(directory)
        directory.mkdir()
        yield JobMock(server, directory, caplog)
        
@pytest.yield_fixture(params=('local','drmaa'))
def job_mock(request, context, test_conf, caplog):
    with JobMockFixture(request.param, context, test_conf, caplog) as job_mock:
        yield job_mock
        
class TestJob(object):
    
    '''Test with any server, using a job mock'''
    
    @pytest.mark.asyncio
    @pytest.mark.parametrize('server_type', ('local', 'drmaa'))
    async def test_succeed(self, server_type, context, context2, test_conf, caplog):
        with JobMockFixture(server_type, context, test_conf, caplog) as job_mock:
            # when cache miss, run
            with job_mock.assert_log(['started', 'finished']):
                await job_mock.run()
                
            # when finished, don't rerun (cache)
            with job_mock.assert_log([]):
                await job_mock.run()
        
        # don't even rerun across application runs (persist)
        with JobMockFixture(server_type, context2, test_conf, caplog) as job_mock:
            with job_mock.assert_log([]):
                await job_mock.run()
            
    @pytest.mark.asyncio
    async def test_fail(self, job_mock):
        # When job exits non-zero, it raises
        job_mock.action = 'fail'
        with job_mock.assert_log(['started', 'failed']):
            with pytest.raises(Exception):
                await job_mock.run()
        
        # When job recovers, it runs and finishes fine
        job_mock.action = 'succeed'
        with job_mock.assert_log(['started', 'finished']):
            await job_mock.run()
        
    @pytest.mark.asyncio
    async def test_cancel(self, job_mock):
        # When job cancelled, it raises asyncio.CancelledError
        job_mock.action = 'forever'
        task = asyncio.ensure_future(job_mock.run())
        asyncio.get_event_loop().call_later(3, task.cancel)
        with job_mock.assert_log(['started', 'cancelled']):
            with pytest.raises(asyncio.CancelledError):
                await task
        
        # When job recovers, it finishes fine
        job_mock.action = 'succeed'
        with job_mock.assert_log(['started', 'finished']):
            await job_mock.run()

    @pytest.mark.asyncio
    async def test_succeed_output(self, server, context, test_conf):
        '''
        When success, correct output files
        '''
        job1 = Job('job1', server, ['sh', '-c', 'pwd; echo $$; echo stderr >&2; echo extra; touch file; mkdir dir'])
        
        dir_ = server.get_directory(job1._data.id)
        if isinstance(server, LocalJobServer):
            expected = context.cache_directory / 'jobs' / str(job1._data.id)
        else:
            expected = Path(test_conf['drmaa_jobs_directory']) / str(job1._data.id)
        assert dir_ == expected
             
        assert job1.directory != server.get_directory(job1._data.id)
            
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
                
    @pytest.mark.asyncio
    async def test_on_directory_created(self, server):
        '''
        Call on_directory_created after creating a job's directory, before
        running the job
        '''
        job1 = Job('job1', server, ['sh', '-c', '[ -e file ]; echo $?'], on_directory_created=lambda job: (job.directory / 'file').touch())
        file = job1.directory / 'file'
        assert not file.exists() 
        await job1.run()
        assert file.exists()
                
class TestLocalJobServer(object):
    
    '''
    Tests specific to LocalJobServer
    '''
    
    def test_custom_jobs_directory(self, context):
        server = LocalJobServer(context, Path('jobs'))
        assert server.get_directory(1) == Path('jobs/1')
          
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
