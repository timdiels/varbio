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
Test deep_genome.core.pipeline._drmaa
'''

from .common import assert_task_log, assert_is_read_only
from deep_genome.core.pipeline import ExitCodeError, Job
from chicken_turtle_util import path as path_
from pathlib import Path
from contextlib import contextmanager
import os
import asyncio
import pytest
import logging

_logger = logging.getLogger(__name__)

try:
    import drmaa
    _drmaa_import_error = None
except RuntimeError as ex:
    _drmaa_import_error = ex

def assert_no_live_jobs(context):
    drmaa_session = context.pipeline._drmaa_session
    if not drmaa_session:
        return
    try:
        # this is as close as it gets, there is no way to list all jobs; unless we add something to DRMAA server to keep track of it.
        # if timeout is exceeded, there is definitely something running. We miss the ones that terminate within the next second though.
        drmaa_session.synchronize([drmaa.Session.JOB_IDS_SESSION_ALL], 1, True)
    except drmaa.ExitTimeoutException:
        drmaa_session.control(drmaa.Session.JOB_IDS_SESSION_ALL, drmaa.JobControlAction.TERMINATE)
        assert False, 'Test left jobs running'
        
@pytest.fixture
def jobs_directory(test_conf):
    if _drmaa_import_error:
        pytest.skip('No DRMAA server')
    jobs_directory = Path(test_conf['main']['drmaa_jobs_directory'])
    path_.remove(jobs_directory, force=True)
    jobs_directory.mkdir()
    return jobs_directory

@pytest.yield_fixture(autouse=True)
def use_drmaa(context, jobs_directory):
    context.initialise_pipeline(jobs_directory, max_cores_used=10)
    yield
    assert_no_live_jobs(context)
    
class JobMock(object):
    
    def __init__(self, context, directory, caplog, version):
        self._inhibitor = directory / 'inhibitor'
        self._file = directory / 'fail'
        self._token = 'jfpw39wuiurjw8w379jfosfus2e7edjf'
        self._caplog = caplog
        wait_for_rm = 'while [ -e "{}" ]; do sleep 1; done'.format(self._inhibitor)
        self._job = context.pipeline.drmaa_job(
            'job1',
            [
                'sh', '-c',
                '{} ; echo {}; [ ! -e {} ]'
                .format(wait_for_rm, self._token, self._file)
            ],
            version=version
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
            
    def assert_log(self, events):
        return assert_task_log(self._caplog, 'drmaa_job', 1, events)
    
    def __getattr__(self, attr):
        return getattr(self._job, attr)
            
@contextmanager
def JobMockFixture(context, caplog, directory, version=1):
    os.makedirs(str(directory), exist_ok=True)
    yield JobMock(context, directory, caplog, version)

@pytest.yield_fixture
def job_mock(context, caplog, jobs_directory):
    directory = jobs_directory / 'test1'
    with JobMockFixture(context, caplog, directory) as job_mock:
        yield job_mock
        
@pytest.fixture
def context2(context2, jobs_directory):
    context2.initialise_pipeline(jobs_directory, max_cores_used=10)
    return context2

@pytest.mark.asyncio
async def test_succeed(context, context2, caplog, jobs_directory):
    with JobMockFixture(context, caplog, jobs_directory / 'context1') as job_mock:
        # when cache miss, run
        with job_mock.assert_log(['started', 'finished']):
            await job_mock.run()
            
        # when finished, don't rerun (cache)
        with job_mock.assert_log([]):
            await job_mock.run()
    context.dispose()
    
    # don't even rerun across application runs (persist)
    with JobMockFixture(context2, caplog, jobs_directory / 'context2') as job_mock:
        with job_mock.assert_log([]):
            await job_mock.run()
        
@pytest.mark.asyncio
async def test_fail(job_mock):
    # When job exits non-zero, it raises
    job_mock.action = 'fail'
    with job_mock.assert_log(['started', 'failed']):
        with pytest.raises(ExitCodeError):
            await job_mock.run()
    
    # When job recovers, it runs and finishes fine
    job_mock.action = 'succeed'
    with job_mock.assert_log(['started', 'finished']):
        await job_mock.run()
    
@pytest.mark.asyncio
async def test_asyncio_cancel(job_mock, context):
    # When job cancelled, it raises asyncio.CancelledError
    job_mock.action = 'forever'
    task = asyncio.ensure_future(job_mock.run())
    asyncio.get_event_loop().call_later(3, task.cancel)
    with job_mock.assert_log(['started', 'cancelling', 'cancelled']):
        with pytest.raises(asyncio.CancelledError):
            await task
    
    # and is no longer running
    assert_no_live_jobs(context)
    
    # When job recovers, it finishes fine
    job_mock.action = 'succeed'
    with job_mock.assert_log(['started', 'finished']):
        await job_mock.run()

@pytest.mark.asyncio
async def test_succeed_output(context, jobs_directory):
    '''
    When success, correct output files
    '''
    job1 = context.pipeline.drmaa_job('job1', ['sh', '-c', 'pwd; echo $$; echo stderr >&2; echo extra; touch file; mkdir dir'])
    assert jobs_directory in job1.directory.parent.parents
        
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
    assert_is_read_only(job1.directory.parent)
            
@pytest.mark.asyncio
async def test_drmaa_cancel(event_loop, context):
    '''
    When cancel job through other interface (i.e. not DG pipeline), still gracefully raise
    '''
    # Note: we don't actually kill from another process, but DG core will still have no idea, which is the point
    
    # Kill job after 3 sec
    def kill_job():
        event_loop.run_in_executor(None, context.pipeline._drmaa_session.control, drmaa.Session.JOB_IDS_SESSION_ALL, drmaa.JobControlAction.TERMINATE)
    event_loop.call_later(3, kill_job)
    
    # Run job
    job1 = context.pipeline.drmaa_job('job1', ['sleep', '99999999999'])
    with pytest.raises(Exception):
            await job1.run()

@pytest.mark.asyncio
async def test_version(context, context2, caplog, jobs_directory):
    '''
    When version changes, rerun even if cached, then fetch from cache because version matches
    '''
    
    with JobMockFixture(context, caplog, jobs_directory / 'tmp1') as job_mock:
        # when cache miss, run
        with job_mock.assert_log(['started', 'finished']):
            await job_mock.run()
    
    with JobMockFixture(context2, caplog, jobs_directory / 'tmp2', version=2) as job_mock:
        # when cache miss (different version), run
        with job_mock.assert_log(['started', 'finished']):
            await job_mock.run()
            
        # when finished, don't rerun (cache)
        with job_mock.assert_log([]):
            await job_mock.run()
            
@pytest.mark.asyncio
async def test_max_cores_used(context, caplog, job_mock, mocker):
    '''
    When threatening to exceed max_cores_used, wait until enough cores are free
    '''
    # Set up events on Job._run
    original_run = Job._run
    events_condition = asyncio.Condition()
    events = []
    async def _run(self):
        events.append((self._id, 'started'))
        async with events_condition:
            events_condition.notify_all()
        
        await original_run(self)
        
        events.append((self._id, 'finished'))
        async with events_condition:
            events_condition.notify_all()
    mocker.patch.object(Job, '_run', _run)
    
    # Set up both jobs
    job_mock.action = 'forever'
    job2 = context.pipeline.drmaa_job('job2', ['true'], cores=10)
    
    # Start running a job forever
    asyncio.ensure_future(job_mock.run())
    async with events_condition:
        events_condition.wait_for(lambda: (1, 'started') in events)
    
    # Run a job that won't fit. I.e. it won't start yet
    job2_future = asyncio.ensure_future(job2.run())
    await asyncio.sleep(1)
    assert (1, 'finished') not in events  # test the test
    assert (2, 'started') not in events
    
    # Let job1 finish, and then job2 will start and finish
    job_mock.action = 'succeed'
    await job2_future
    
    # Assert events
    expected = [
        (job_mock._id, 'started'),
        (job_mock._id, 'finished'),
        (job2._id, 'started'),
        (job2._id, 'finished'),
    ]
    assert events == expected

