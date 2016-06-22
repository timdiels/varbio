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

from deep_genome.core.pipeline import LocalJobServer, DRMAAJobServer, pipeline_cli, TaskFailedError, Job, Task
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
    server = DRMAAJobServer(jobs_directory, context)
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

def _normalise_event(event):
    job, action = event
    return (job.name, action)

@contextmanager
def assert_execution_order(caplog, events):
    '''
    Assert jobs run/finish in the given order
    '''
    original_count = len(caplog.text().splitlines())
    with caplog.atLevel(logging.INFO, logger='deep_genome.core.pipeline'):
        yield
    lines = caplog.text().splitlines()[original_count:]
    
    # normalise events dict
    events_ = {}
    for event, dependencies in events.items():
        event = _normalise_event(event)
        if not dependencies:
            dependencies = set()
        elif isinstance(dependencies, tuple):
            dependencies = {dependencies}
        dependencies = {_normalise_event(event) for event in dependencies}
        events_[event] = dependencies
    events = events_
    del events_
    
    # assert
    events_seen = set()
    for line in lines:
        match = re.search(r"Task '(.+)': (started|failed|finished|cancelled)", line)
        if match:
            event = (match.group(1), match.group(2))
            assert event not in events_seen, 'Event happens twice'
            assert not events[event] - events_seen, 'Event happens before its dependencies'
            events_seen.add(event)
    missing_events = events.keys() - events_seen
    if missing_events:
        assert False, 'Events did not happen: ' + ', '.join(map(str, missing_events)) 

def sh(command):
    return ['sh', '-c', command]

def wait_for_rm(path):
    return 'while [ -e "{}" ]; do sleep 1; done'.format(path)

def ps_aux_contains(term):
    for line in pb.local['ps']('aux').splitlines():
        if term in line:
            return True
    return False

class TestTask(object):

    @pytest.mark.parametrize('name', ('.', '..', "name'quot", 'name"dquot', 'ay/lmao', 'jobbu~1', ' ', '  ', '\t', ' leading.space', '\tleading.space', 'trailing.space ', 'trailing.space\t', '3no'))
    def test_invalid_name(self, context, name):
        '''
        When insane name, raise
        '''
        with pytest.raises(ValueError) as ex:
            Task(name, context)
        assert 'name' in str(ex.value)
        assert 'valid' in str(ex.value)
        
    @pytest.mark.parametrize('name', ('name', 'n1', "name.hi", 'name2.hi1', '_1', 'mix.max_._hi._1be'))
    def test_valid_name(self, context, name):
        '''
        When insane name, raise
        '''
        Task(name, context)
        
class TestJob(object):
    
    def test_job_interface(self, local_job_server):
        # The interface for defining jobs
        job1 = Job('job1', local_job_server, ['true'])
        assert job1.name == 'job1'
        job2 = Job('job2', local_job_server, ['true'], dependencies={job1})
        assert job2.name == 'job2'
    
class TestJobServer(object):
    
    '''
    Test Job in combination with each type of job server
    '''
    
    @pytest.mark.asyncio
    async def test_succeed(self, server, context, test_conf):
        '''
        When succesful job, call with correct context and report success
        '''
        job1 = Job('job1', server, sh('pwd; echo $$; echo stderr >&2; echo extra; touch file; mkdir dir'))
        
        dir_ = server.get_directory(job1)
        if isinstance(server, LocalJobServer):
            assert dir_ == context.cache_directory / 'jobs' / job1.name
        else:
            assert dir_ == Path(test_conf['drmaa_jobs_directory']) / job1.name
             
        assert job1.directory != server.get_directory(job1)
            
        # Run
        assert not job1.finished
        await job1.run()
        assert job1.finished
        
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
    async def test_fail(self, server):
        '''
        When job exits non-zero, raise
        '''
        job1 = Job('job1', server, sh('exit 1'))
        with pytest.raises(TaskFailedError):
            await job1.run()
        
    @pytest.mark.asyncio
    async def test_run_before_finishing_dependencies(self, server, caplog):
        '''
        When running a job before its dependencies have finished, run its dependencies as well
        '''
        job2 = Job('job2', server, ['true'])
        job1 = Job('job1', server, ['true'], dependencies={job2})
        order = {
            (job2, 'started') : (),
            (job2, 'finished') : (job2, 'started'),
            (job1, 'started') : (job2, 'finished'),
            (job1, 'finished') : (job1, 'started')
        }
        with assert_execution_order(caplog, order):
            await job1.run()
    
    @pytest.mark.asyncio    
    async def test_run_while_running(self, server):
        '''
        When running a job while it is already running, return the same future
        '''
        job1 = Job('job1', server, ['true'])
        future = job1.run()
        assert job1.run() == future  # return the same future
        
        # cleanup: asyncio doesn't like unused futures
        await future

    @pytest.mark.asyncio
    async def test_cannot_run_when_finished(self, server):
        '''
        When trying to run a job that has finished, raise
        '''
        job1 = Job('job1', server, ['true'])
        await job1.run()
        with pytest.raises(InvalidOperationError) as ex:
            job1.run()
        assert 'Cannot run a finished job' in str(ex.value)
        
@pytest.mark.asyncio
async def test_persistence(context, context2):
    '''
    When a job has finished, remember it across runs
    '''
    job1 = Job('job1', LocalJobServer(context), ['true'])
    await job1.run()
    
    job1_ = Job('job1', LocalJobServer(context2), ['true'])
    assert job1.name == job1_.name
    assert job1_.finished
        
class TestExecutionOrder(object):
    
    '''
    Test jobs run in the right order and with the right concurrency
    '''
    
    @pytest.fixture
    def fake_job(self, server):
        return Job('fake', server, ['true'])
    
    def create_inhibitor(self, path):
        os.makedirs(str(path.parent), exist_ok=True)
        path.touch()
        
    @pytest.fixture
    def inhibitor1(self, server, fake_job):
        path = fake_job.directory / 'inhibitor1'
        self.create_inhibitor(path)
        return path
    
    @pytest.fixture
    def inhibitor2(self, server, fake_job):
        path = fake_job.directory / 'inhibitor2'
        self.create_inhibitor(path)
        return path
    
    @pytest.fixture
    def inhibitor3(self, server, fake_job):
        path = fake_job.directory / 'inhibitor3'
        self.create_inhibitor(path)
        return path

    @pytest.mark.asyncio
    async def test_success(self, server, caplog, inhibitor1, inhibitor2, inhibitor3):
        '''
        When no failing jobs, execute job tree from scratch in the right order and concurrently where possible
        '''
        # setup:
        #
        # job2 --> job5 -> job6
        # job3 -/      /
        # job4 -------/
        #  
        # job2 doesn't end before job3 starts
        # job3 doesn't end before job2 starts
        # job4 ends after job5 starts
        job2 = Job('job2', server, sh('rm {}; {}'.format(inhibitor1, wait_for_rm(inhibitor2))))
        job3 = Job('job3', server, sh('rm {}; {}'.format(inhibitor2, wait_for_rm(inhibitor1))))
        job4 = Job('job4', server, sh(wait_for_rm(inhibitor3)))
        job5 = Job('job5', server, sh('rm {}'.format(inhibitor3)), dependencies={job2, job3})
        job6 = Job('job6', server, ['true'], dependencies={job4, job5})
        
        # assert
        order = {
            (job2, 'started') : (),
            (job3, 'started') : (),
            (job4, 'started') : (),
            (job2, 'finished') : {(job2, 'started'), (job3, 'started')},
            (job3, 'finished') : {(job2, 'started'), (job3, 'started')},
            (job5, 'started') : {(job2, 'finished'), (job3, 'finished')},
            (job4, 'finished') : (job5, 'started'),
            (job5, 'finished') : (job5, 'started'),
            (job6, 'started') : {(job4, 'finished'), (job5, 'finished')},
            (job6, 'finished') : (job6, 'started')
        }
        with assert_execution_order(caplog, order):
            await job6.run()
        
    @pytest.mark.asyncio
    async def test_fail(self, context, server, caplog, temp_dir_cwd, inhibitor1):
        '''
        Test job failure handling
        
        - When a job fails, continue running the job tree as far as possible without it.
        - When resumed, continue with the failed jobs
        '''
        # job1 -> job3 \
        # job2 -> job4 -> job5
        # job2 fails if run before job3 finishes
        # job1 does not finish before job2 nearly finishes 
        job1 = Job('job1', server, sh(wait_for_rm(inhibitor1)))
        job3 = Job('job3', server, sh('touch done'), dependencies={job1})
        job2 = Job('job2', server, sh('[ -e "{}" ]; exists=$?; rm {}; exit $exists'.format(job3.directory / 'done', inhibitor1)))
        job4 = Job('job4', server, ['true'], dependencies={job2})
        job5 = Job('job5', server, ['true'], dependencies={job3, job4})
        
        # assert
        order = {
            (job1, 'started') : (),
            (job2, 'started') : (),
            (job2, 'failed') : (job2, 'started'),
            (job1, 'finished') : {(job1, 'started'), (job2, 'started')},
            (job3, 'started') : (job1, 'finished'),
            (job3, 'finished') : (job3, 'started')
        }
        with assert_execution_order(caplog, order):
            with pytest.raises(TaskFailedError) as ex:
                await job5.run()
            assert "Dependency '{}' failed".format(job4.name) in str(ex.value)
            
        # When job2 no longer fails, finish all the rest
        order = {
            (job2, 'started') : (),
            (job2, 'finished') : (job2, 'started'),
            (job4, 'started') : (job2, 'finished'),
            (job4, 'finished') : (job4, 'started'),
            (job5, 'started') : (job4, 'finished'),
            (job5, 'finished') : (job5, 'started')
        }
        with assert_execution_order(caplog, order):
            await job5.run()
        
    def test_cancel(self, context, server, caplog, event_loop, inhibitor1, inhibitor2):
        '''
        When stopped, the running jobs are stopped and the pipeline is resumed from the stopped jobs the next time
        '''
        # job1 -> job2 -> job3
        # canceller waits for start of job2
        # job2 does not finish first run, but does finish second run
        token = 'magic32091373831920313903651230536829294432789637373'  # something likely unique to search for
        job1 = Job('job1', server, ['true'])
        job2 = Job('job2', server, dependencies={job1}, command=sh('echo {}; rm {}; {} || true'.format(token, inhibitor1, wait_for_rm(inhibitor2))))
        job3 = Job('job3', server, ['true'], dependencies={job2})
        
        async def canceller():
            while inhibitor1.exists():
                await asyncio.sleep(1)
            job3.cancel()
        
        # When calling cancel when not running, ignore call
        job1.cancel()
        
        # When call stop, stop
        order = {
            (job1, 'started') : (),
            (job1, 'finished') : (job1, 'started'),
            (job2, 'started') : (job1, 'finished'),
            (job2, 'cancelled') : (job2, 'started'),
            (job3, 'cancelled') : (job2, 'started')
        }
        with assert_execution_order(caplog, order):
            done, _ = event_loop.run_until_complete(asyncio.wait((job3.run(), canceller())))
            with pytest.raises(asyncio.CancelledError):
                for future in done:
                    future.result()
            assert job1.finished
            assert not job2.finished # cancel should have hit during job2 execution and thus job2 should not have finished
            
            # check it is indeed no longer running
            if isinstance(server, LocalJobServer):
                assert not ps_aux_contains(token)
            else:
                assert_no_live_jobs(server)
        
        # When calling stop when finished, ignore call
        job1.cancel()
        
        # When resume, pick up from last time until finish
        inhibitor2.unlink()  # this time allow job2 to finish
        order = {
            (job2, 'started') : (),
            (job2, 'finished') : (job2, 'started'),
            (job3, 'started') : (job2, 'finished'),
            (job3, 'finished') : (job3, 'started')
        }
        with assert_execution_order(caplog, order):
            event_loop.run_until_complete(job3.run())
          
class TestDRMAAJobServer(object):
    
    '''
    Tests specific to DRMAAJobServer
    '''
    
    @pytest.mark.asyncio
    async def test_out_of_band_cancel(self, drmaa_job_server, event_loop):
        '''
        When somebody other process cancels our job, gracefully raise TaskFailedError
        '''
        # Note: we don't actually kill from another process, but DG core will still have no idea, which is the point
        
        # Kill job after 3 sec
        def kill_job():
            event_loop.run_in_executor(None, drmaa_job_server._session.control, drmaa.Session.JOB_IDS_SESSION_ALL, drmaa.JobControlAction.TERMINATE)
        event_loop.call_later(3, kill_job)
        
        # Run job
        job1 = Job('job1', drmaa_job_server, ['sleep', '99999999999'])
        with pytest.raises(TaskFailedError):
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
