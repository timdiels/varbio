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

import asyncio
import logging
import os
from chicken_turtle_util import path as path_
from chicken_turtle_util.exceptions import InvalidOperationError
from pathlib import Path
import plumbum as pb
from contextlib import suppress

# Note: Libraries other than drmaa I considered:
# - gridmap (GPL, thus not compatible with our LGPL project)
# - https://bitbucket.org/markoristin/a-python-wrapper-for-sun-grid-engine (GPL)
# - https://github.com/hbristow/gridengine (GPL)
# - https://github.com/jiahao/PySGE (no setup.py)
try:
    import drmaa
    _drmaa_import_error = None
except RuntimeError as ex:  # drmaa isn't always used, don't complain when it fails to load
    _drmaa_import_error = ex

logger = logging.getLogger(__name__)
    
class Job(object):
    
    '''
    A job with a command that can be submitted to a job server for execution
    
    The job can be run by awaiting it. It can be awaited at most once.
    
    Parameters
    ----------
    command : [any]
        ``str(command[0])`` is the executable (script with shebang or binary)
        to execute, ``map(str, command[1:])`` are the args to pass it. The
        executable is looked up using the PATH env var if it's not absolute.
    server : JobServer
        Server to submit job to for execution.
    name : str
        Unique job name. May use any characters, even whitespace (and nul
        characters), but it's recommended to keep it readable.
        
        Avoid using counters to make names unique, one would then have to be
        careful the numbers are assigned in the same order on the next run. For
        example, when creating similar tasks, use the creation arguments in the
        name::
        
            def create_job(server, arg1, arg2):
                name = 'my.package.create_job({!r}, {!r})'.format(arg1, arg2)
                return Job(name, server, ['command', arg1, arg2])
                
        Or equivalently using `deep_genome.core.pipeline.format_call`::
        
            def create_job(server, arg1, arg2):
                name = format_call(create_job, arg1, arg2)
                return Job(name, server, ['command', arg1, arg2])
                
    server_args : str
        Additional arguments specific to the job server. E.g. in DRMAAJobServer
        this corresponds to the `native specification`_, which in the case of
        SGE or OGS is a string of options given to qsub (according to
        http://linux.die.net/man/3/drmaa_attributes).
        
        .. _native specification: http://gridscheduler.sourceforge.net/javadocs/org/ggf/drmaa/JobTemplate.html#setNativeSpecification(java.lang.String)
    '''
    
    # Note: If you get "drmaa.errors.DeniedByDrmException: code 17: error: no
    # suitable queues" consider prefixing the server_args with '-w no'. This
    # most often happens when specifying mem_free on clusters that aren't
    # configured for it (see 
    # http://gridscheduler.sourceforge.net/howto/commonproblems.html#interactive)
    
    def __init__(self, name, server, command, server_args=None):
        self._context = server.context
        command = [str(x) for x in command]
        self._executable = Path(str(pb.local[command[0]].executable))
        self._args = command[1:]
        self._server = server
        self._server_args = server_args
        
        if name in self._context._jobs:
            raise ValueError('A Job already exists with this name: ' + name)
        self._context._jobs[name] = self
        self._name = name
        
        # Load/create from database 
        with self._context.database.scoped_session() as session:
            sa_session = session.sa_session
            job = sa_session.query(self._db.e.Job).filter_by(name=self._name).one_or_none()
            if not job:
                job = self._db.e.Job(name=name, finished=False)
                sa_session.add(job)
                sa_session.flush()
            self._id = job.id
            self._finished = job.finished
            
    @property
    def _db(self):
        return self._context.database
    
    @property
    def id(self):
        return self._id
    
    @property
    def executable(self):
        return self._executable
    
    @property
    def args(self):
        return self._args
    
    @property
    def server_args(self):
        return self._server_args
    
    @property
    def directory(self):
        '''
        Get working directory
        '''
        return self._server.get_directory(self) / 'output'
    
    @property
    def stderr_file(self):
        '''
        Returns
        -------
        pathlib.Path
        '''
        return self._server.get_directory(self) / 'stderr'
    
    @property
    def stdout_file(self):
        '''
        Returns
        -------
        pathlib.Path
        '''
        return self._server.get_directory(self) / 'stdout'
    
    async def run(self):
        '''
        Run job on server
        
        If job has already been run successfully before, return immediately.
        '''
        if self._finished:
            logger.debug("Job {} already finished, not rerunning. Name: {}".format(self._id, self._name))
            return
        try:
            logger.info("Job {} started. Name: {}".format(self._id, self._name))
            await self._server.run(self)
            self._finished = True
            with self._context.database.scoped_session() as session:
                job = session.sa_session.query(self._db.e.Job).get(self._id)
                assert job
                job.finished = True
            logger.info("Job {} finished. Name: {}".format(self._id, self._name))
        except asyncio.CancelledError:
            logger.info("Job {} cancelled. Name: {}".format(self._id, self._name))
            raise
        except Exception as ex:
            logger.info("Job {} failed. Name: {}".format(self._id, self._name))
            raise
            
    def __str__(self):
        max_ = 100
        if len(self._name) > max_:
            name = self._name[:max_] + '...'
        else:
            name = self._name
        return 'Job(id={}, name={!r})'.format(self._id, name)
    
    def __repr__(self):
        return 'Job(id={!r})'.format(self._id)
        
class JobServer(object):
    
    def __init__(self, context):
        self._context = context
        
    @property
    def context(self):
        return self._context
    
    def get_directory(self, job):
        '''
        Get directory in which a job's data is stored
        '''
        raise NotImplementedError()
    
    async def run(self, job):
        '''
        Internal, use `Job.run` instead. Add job to queue and run it
        
        Parameters
        ----------
        job : Job
        '''
        path_.remove(self.get_directory(job), force=True)  # remove left overs from a previous (failed) run
        os.makedirs(str(job.directory), exist_ok=True)
        
        await self._run(job)
    
        # Make job data dir read only
        for dir_, _, files in os.walk(str(self.get_directory(job))):
            dir_ = Path(dir_)
            dir_.chmod(0o500)
            for file in files:
                (dir_ / file).chmod(0o400)
        
class LocalJobServer(JobServer):
    
    '''
    Runs jobs locally
    
    Parameters
    ----------
    context : deep_genome.core.Context
    jobs_directory : pathlib.Path or None
        Directory in which to create working directories for jobs. If ``None``,
        a subdirectory of XDG_CACHE_DIR will be used.
    '''
    
    def __init__(self, context, jobs_directory=None):
        super().__init__(context)
        if jobs_directory:
            self._jobs_directory = jobs_directory
        else:
            self._jobs_directory = self._context.cache_directory / 'jobs'
        
    def get_directory(self, job):
        return self._jobs_directory / str(job.id)
        
    async def _run(self, job): # assuming a fresh job dir, run
        with job.stdout_file.open('w') as stdout:
            with job.stderr_file.open('w') as stderr:
                args = [str(job.executable)] + job.args
                process = await asyncio.create_subprocess_exec(*args, cwd=str(job.directory), stdout=stdout, stderr=stderr)
                try:
                    return_code = await process.wait()
                except asyncio.CancelledError:
                    await _kill(process.pid)
                    raise
                if return_code != 0:
                    raise Exception('Job {} exited with non-zero exit code: {}'.format(job.id, return_code))
    
class DRMAAJobServer(JobServer):
    
    '''
    Submits jobs to a cluster via a DRMAA interface on localhost.
    
    You may instantiate at most one DRMAAJobServer.
    
    Cluster software that supports the DRMAA interface: https://www.drmaa.org/implementations.php
    
    Parameters
    ----------
    context : deep_genome.core.Context
    jobs_directory : pathlib.Path
        Directory in which to create working directories for jobs. Should be
        accessible on both the local machine and the cluster workers.
    '''
    
    _session = None
    
    def __init__(self, context, jobs_directory):
        if _drmaa_import_error:
            raise _drmaa_import_error
        super().__init__(context)
        if self._session:
            raise InvalidOperationError("Cannot instantiate multiple `DRMAAJobServer`s")
        self._jobs_directory = jobs_directory
        DRMAAJobServer._session = drmaa.Session()
        DRMAAJobServer._session.initialize()
        
    def get_directory(self, job):
        return self._jobs_directory / str(job.id)
    
    async def _run(self, job): # assuming a fresh job dir, run
        loop = asyncio.get_event_loop()
        
        # Submit job
        job_template = DRMAAJobServer._session.createJobTemplate()
        try:
            job_template.workingDirectory = str(job.directory)
            job_template.outputPath = ':' + str(job.stdout_file)
            job_template.errorPath = ':' + str(job.stderr_file)
            job_template.remoteCommand = str(job.executable)
            job_template.args = job.args
            if job.server_args:
                job_template.nativeSpecification = job.server_args
            job_id = await loop.run_in_executor(None, DRMAAJobServer._session.runJob, job_template)
        finally:
            DRMAAJobServer._session.deleteJobTemplate(job_template)
            
        # Wait for job
        try:
            result = await loop.run_in_executor(None, DRMAAJobServer._session.wait, job_id, drmaa.Session.TIMEOUT_WAIT_FOREVER)
        except asyncio.CancelledError as ex:
            with suppress(drmaa.errors.InvalidJobException):  # perhaps job_id has already disappeared from server (because it finished or was terminated?)
                await loop.run_in_executor(None, DRMAAJobServer._session.control, job_id, drmaa.JobControlAction.TERMINATE)  # May return before job is actually terminated
                await loop.run_in_executor(None, DRMAAJobServer._session.wait, job_id, drmaa.Session.TIMEOUT_WAIT_FOREVER)  # So try to wait for termination
            raise ex
        
        # Check result
        if result.wasAborted:
            raise Exception('Job {} was aborted before it even started running'.format(job.id))
        elif result.hasSignal:
            raise Exception('Job {} was killed with signal {}'.format(job.id, result.terminatedSignal))
        elif not result.hasExited:
            raise Exception('Job {} did not exit normally'.format(job.id))
        elif result.hasExited and result.exitStatus != 0:
            raise Exception('Job {} exited with non-zero exit code: {}'.format(job.id, result.exitStatus))
        logger.debug("Job {}'s resource usage was: {}".format(job.id, result.resourceUsage))
        
    def dispose(self):
        '''
        Dispose of this instance (allows creating a new DRMAAJobServer afterwards)
        '''
        DRMAAJobServer._session.exit()
        DRMAAJobServer._session = None
    
async def _kill(pid, timeout=10):
    '''
    Kill process and its children and wait for them to terminate
    
    First sends SIGTERM, then after a timeout sends SIGKILL.
    
    Parameters
    ----------
    pid
        PID of parent
    timeout : int
        Timeout in seconds before sending SIGKILL.
    '''
    import psutil
    parent = psutil.Process(pid)
    processes = [process for process in list(parent.children(recursive=True))] + [parent]
    for process in processes:
        with suppress(psutil.NoSuchProcess):
            process.terminate()
    _, processes = await asyncio.get_event_loop().run_in_executor(None, psutil.wait_procs, processes, timeout)
    if processes:
        logger.warning('Process did not terminate within timeout, sending SIGKILL')
        for process in processes:
            with suppress(psutil.NoSuchProcess):
                process.kill()