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

# TODO swap order on (context, job_dir)
# TOOD allow custom jobs dire on LocalJobServer but default to cache dir

# TODO allow any chars in name, it is now a string id
# - with characters that would be invalid as filename. It could also become too
# long. Probably shouldn't use it as a dir name. Must add in database and use 
# the db's task id as directory name. This will make it hell to debug though,
# but I see no other way
# - __str__ shouldn't show it, at least not fully. 
# - __repr__ should show it as that's what a good repr does.

# Remove concept of Job and Task dependencies, one can simply await from `Task`s instead
#TODO not always wrapped in TaskFailedError. Why bother with TaskFailedError in the first place?
#TODO logging: why not: started, waiting for, resumed, waiting for, ... A kind of thing you could see
#TODO no deps at all on Job? Simply let the user handle it? E.g. in a task, call await for jobs, then continue etc? Very reasonable thing to do actually!

'''
Utilities for building a pipeline

A pipeline is a set of jobs, with interdependencies.

A job can be run locally or on cluster via a DRMAA interface (e.g. Open Grid
Scheduler supports DRMAA). Each job represents a call to an executable with some
arguments, to be run locally or on the cluster. Each job is given its own clean
working directory (even when it was cancelled before, the next try will start
from a fresh directory again).

To run some Python code on the controller machine, use SimpleTask. When unable
to define your dependencies up front, use Task. To run an executable (in a
subprocess) on the controller machine, use a Job with a LocalJobServer. To run
an executable on a cluster (with an DRMAA interface) use Job with a
DRMAAJobServer. You should make no more than 1 instance of each type of job server.
'''

import asyncio
import logging
import signal
import os
import re
from chicken_turtle_util import path as path_
from chicken_turtle_util.exceptions import InvalidOperationError
from deep_genome.core.database import entities
from pathlib import Path
import plumbum as pb
from pprint import pformat
from contextlib import suppress
import traceback
import sys

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

# Note: jobs can be part of multiple pipelines. E.g. J1 <-- J2 and J1 <-- J3.

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
                
async def _async_noop():
    pass

class TaskFailedError(Exception):
    pass

class Task(object):
    
    '''
    A task with dependencies and a persisted completion state.
    
    Parameters
    ----------
    context
    name : str
        Unique task name. Valid names are like Python packages, but less strict,
        see below. The name does not have to refer to actual packages.
        
        If using the same database for multiple pipeline projects it is
        recommended to prefix it with your project's package name to avoid
        clashes with the other projects. Avoid using counters to make names
        unambiguous, one would then have to be careful the numbers are assigned
        in the same order on the next run; instead, specify some args in the
        name; e.g. ``f(x): return MyTask(name='base.name(x={})'.format(x)``.
        
        Names are like Python packages, but identifiers may also use the ``(
        =,)`` characters. An identifier must start with a letter and may not
        have trailing whitespace. Formally::
            
            identifier := [_a-zA-Z][_a-zA-Z0-9( =,)]*
            name := {identifier}([.]{identifier})*
            name not in ('.', '..')
            name == name.strip()
    '''
    
    def __init__(self, name, context):
        self.__context = context
        self.__run_task = None  # when running, this contains the task that runs the job
        
        identifier = r'[_a-zA-Z][_a-zA-Z0-9( =,)]*'
        pattern = r'{identifier}([.]{identifier})*'.format(identifier=identifier)
        if name in ('.', '..') or name != name.strip() or not re.fullmatch(pattern, name):
            raise ValueError('Invalid task name: ' + name)
        if name in context.tasks:
            raise ValueError('A task already exists with this name: ' + name)
        self.__name = name
        
        # finished 
        with self.__context.database.scoped_session() as session:
            self.__finished = session.sa_session.query(entities.Task).filter_by(name=self.name).one_or_none() is not None
    
    @property
    def _context(self):
        return self.__context
            
    @property
    def name(self):
        '''
        Get unique task name
        
        Syntax: like a package name.
        '''
        return self.__name
            
    @property
    def finished(self):
        return self.__finished
    
    def run(self):
        '''
        Run task and any unfinished dependencies
        
        When a dependency fails, stubbornly waits until the other dependencies
        have finished or failed.
        
        Returns
        -------
        asyncio.Task
            task that runs the job. Task raises `TaskFailedError` when the job or
            one of its dependencies fails to finish.
        ''' # XXX aren't there cases where TaskFailedError is raised although it hasn' begun? E.g. failed dep? Maybe raise diff one for that. DependencyFailedError, e.g.
        if self.finished:
            return asyncio.ensure_future(_async_noop())
        if not self.__run_task:
            self.__run_task = asyncio.ensure_future(self.__run())
        return self.__run_task
    
    async def __run(self):
        try:
            logger.info("Task started: {}".format(self.name))
            await self._run()
            self.__finished = True
            with self.__context.database.scoped_session() as session:
                session.sa_session.add(entities.Task(name=self.name))
            logger.info("Task finished: {}".format(self.name))
        except asyncio.CancelledError:
            logger.info("Task cancelled: {}".format(self.name))
            raise
        except Exception as ex:
            logger.info("Task failed: {}".format(self.name))
            raise TaskFailedError('Task failed') from ex
        finally:
            self.__run_task = None
            
    def cancel(self):
        '''
        Cancel job and any dependencies it started
        '''
        if self.__run_task:
            self.__run_task.cancel()

    async def _run(self):
        '''
        Run task itself
        '''
        raise NotImplementedError()
        
    async def _run_dependencies_(self, dependencies):
        '''
        Helper to wait for dependencies
        
        Parameters
        ----------
        dependencies : iterable(Task)
            Dependencies to run and await until finished
        '''
        dependencies = [dependency for dependency in dependencies if not dependency.finished]
        if dependencies:
            results = await asyncio.gather(*(dependency.run() for dependency in dependencies), return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, asyncio.CancelledError):
                    # Note: gather doesn't raise CancelledError when it is
                    # cancelled, so we must assume that a cancelled child
                    # means we were cancelled. Go back to using asyncio.wait
                    # if that bothers you
                    raise result
                if isinstance(result, Exception):
                    job = dependencies[i]
                    raise TaskFailedError("Dependency '{}' failed".format(job.name)) from result
    
    def __repr__(self):
        return 'Task(name={!r})'.format(self.name)
    
class Job(Task):
    
    '''
    A job with a command that can be submitted to a job server for execution
    
    Concurrently using the same job in multiple DG contexts using the same
    database is not supported.
    
    Parameters
    ----------
    command : [any]
        ``str(command[0])`` is the executable (script with shebang or binary)
        to execute, ``map(str, command[1:])`` are the args to pass it. The
        executable is looked up using the PATH env var if it's not absolute.
    server : JobServer
        Server to submit job to for execution.
    name : str
        See ``help(deep_genome.core.pipeline.Task)``
    server_args : str
        Additional arguments specific to the job server. E.g. in DRMAAJobServer
        this corresponds to the 
        `native specification <http://gridscheduler.sourceforge.net/javadocs/org/ggf/drmaa/JobTemplate.html#setNativeSpecification(java.lang.String)>`_.
    '''
    
    def __init__(self, name, server, command, server_args=None):
        super().__init__(name, server.context)
        command = [str(x) for x in command]
        self._executable = Path(str(pb.local[command[0]].executable))
        self._args = command[1:]
        
        self._server = server
        self._server_args = server_args
    
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
    
    async def _run(self):
        await self._server.run(self)
        
    def __repr__(self):
        return 'Job(name={!r})'.format(self.name)
        
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
        Add job to queue and run it
        
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
    '''
    
    def __init__(self, context):
        super().__init__(context)
        
    def get_directory(self, job):
        return self._context.cache_directory / 'jobs' / job.name
        
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
                    raise TaskFailedError('Non-zero exit code: {}'.format(return_code))
    
class DRMAAJobServer(JobServer):
    
    '''
    Submits jobs to a cluster via a DRMAA interface on localhost.
    
    You may instantiate at most one DRMAAJobServer.
    
    Cluster software that supports the DRMAA interface: https://www.drmaa.org/implementations.php
    
    Parameters
    ----------
    jobs_directory : pathlib.Path
        Directory in which to create working directories for jobs. Should be
        accessible on both the local machine and the cluster workers.
    context
    '''
    
    _is_first = True
    
    def __init__(self, jobs_directory, context):
        if _drmaa_import_error:
            raise _drmaa_import_error
        super().__init__(context)
        if self._is_first:
            self._is_first = False
        else:
            raise InvalidOperationError("Cannot instantiate multiple `DRMAAJobServer`s")
        self._jobs_directory = jobs_directory
        self._session = drmaa.Session()
        self._session.initialize()
        
    def get_directory(self, job):
        return self._jobs_directory / job.name
    
    async def _run(self, job): # assuming a fresh job dir, run
        loop = asyncio.get_event_loop()
        
        # Submit job
        job_template = self._session.createJobTemplate()
        try:
            job_template.workingDirectory = str(job.directory)
            job_template.outputPath = ':' + str(job.stdout_file)
            job_template.errorPath = ':' + str(job.stderr_file)
            job_template.remoteCommand = str(job.executable)
            job_template.jobName = job.name
            job_template.args = job.args
            if job.server_args:
                job_template.nativeSpecification = job.server_args
            job_id = await loop.run_in_executor(None, self._session.runJob, job_template)
        finally:
            self._session.deleteJobTemplate(job_template)
            
        # Wait for job
        try:
            result = await loop.run_in_executor(None, self._session.wait, job_id, drmaa.Session.TIMEOUT_WAIT_FOREVER)
        except asyncio.CancelledError as ex:
            with suppress(drmaa.errors.InvalidJobException):  # perhaps job_id has already disappeared from server (because it finished or was terminated?)
                await loop.run_in_executor(None, self._session.control, job_id, drmaa.JobControlAction.TERMINATE)  # May return before job is actually terminated
                await loop.run_in_executor(None, self._session.wait, job_id, drmaa.Session.TIMEOUT_WAIT_FOREVER)  # So try to wait for termination
            raise ex
        
        # Check result
        if result.wasAborted:
            raise TaskFailedError('Job was aborted before it even started running')
        elif result.hasSignal:
            raise TaskFailedError('Job was killed with signal {}'.format(result.terminatedSignal))
        elif not result.hasExited:
            raise TaskFailedError('Job did not exit normally')
        elif result.hasExited and result.exitStatus != 0:
            raise TaskFailedError('Job exited with non-zero exit code: {}'.format(result.exitStatus))
        logger.debug("Job {}'s resource usage was: {}".format(job.name, pformat(result.resourceUsage)))
        
    def dispose(self):
        self._session.exit()
    
def pipeline_cli(Context, create_jobs):
    
    '''
    Get CLICK CLI command to run/resume pipeline
    
    Parameters
    ----------
    Context 
        Application context class to use. Should be
        deep_genome.core.context.AlgorithmMixin or a subclass thereof.
    create_jobs : (Context) -> Job
        callback that is called with an instance of the context you provided. It
        should return the final job that the pipeline should finish (it will run
        dependencies as if final_job.run() was called).
    '''
    
    @Context.command()
    def main(context):
        '''
        Run/resume the pipeline
        
        Fault tolerance:
        
        - When signal interrupted (SIGTERM), stops all jobs and gracefully exits.
        The run can be resumed correctly on a next invocation.
        
        - When killed (SIGKILL), or when server has power failure, or when
        errors like out-of-memory raised, simply crash. Before resuming you
        should kill any jobs started by the pipeline. Jobs that finished before
        SIGKILL arrived, will not be rerun
        '''
        loop = asyncio.get_event_loop()
        final_job = create_jobs(context)
        loop.add_signal_handler(signal.SIGTERM, final_job.cancel)
        try:
            loop.run_until_complete(final_job.run())
        except asyncio.CancelledError:
            print()
            print('Pipeline: run cancelled')
            sys.exit(1)
        except Exception:
            traceback.print_exc()
            print()
            print('Pipeline: run failed to complete')
            sys.exit(1)
        finally:
            loop.close()
        print()
        print('Pipeline: run completed')
    return main
