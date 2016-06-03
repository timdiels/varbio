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
Utilities for building a pipeline

A pipeline is a set of jobs, with interdependencies.

A job can be run locally or on cluster via a DRMAA interface (e.g. Open Grid
Scheduler supports DRMAA). Each job represents a call to an executable with some
arguments, to be run locally or on the cluster. Each job is given its own clean
working directory (even when it was cancelled before, the next try will start
from a fresh directory again).
'''

import networkx as nx
import asyncio
import logging
import signal
import os
from chicken_turtle_util import cli, observable, path as path_
from chicken_turtle_util.exceptions import InvalidOperationError
from deep_genome.core.database import entities
from collections import defaultdict
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

class JobFailedError(Exception):
    pass

class Jobs(object):
    
    '''
    Internal class.
    '''
    
    def __init__(self):
        self._job_base_ids = defaultdict(lambda: -1)
    
    def add(self, job, name):
        self._job_base_ids[name] += 1
        if self._job_base_ids[name] > 0:
            name += '~' + str(self._job_base_ids[name])
        return name
    
class Job(object):
    
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
        Job name. Musn't contain any invalid file-name characters or '~'. If it
        clashes with another job name, a suffix is added: ``~clash_count``.
        Consider prefixing with the package name to avoid clashes. It should be
        prefixed with the project's package name to avoid clashes with jobs in
        other projects using the same DG database.
    dependencies : iterable(Job)
        Jobs that must be finished before this job can start
    server_args : str
        Additional arguments specific to the job server. E.g. in DRMAAJobServer
        this corresponds to the 
        `native specification <http://gridscheduler.sourceforge.net/javadocs/org/ggf/drmaa/JobTemplate.html#setNativeSpecification(java.lang.String)>`_.
    '''
    
    def __init__(self, command, server, name='unnamed', dependencies=(), server_args=None):
        command = [str(x) for x in command]
        self._executable = Path(str(pb.local[command[0]].executable))
        self._args = command[1:]
        
        self._context = server.context  # server and job should have the same context, so we might as well get it off server's
        self._server = server
        self._server_args = server_args
        self._run_task = None  # when running, this contains the task that runs the job
        self._dependencies = observable.Set(dependencies)
        self._dependencies.change_listeners.append(self._on_dependencies_changed)
        
        if name in ('.', '..') or any(char in name for char in '/\0~\'"'):
            raise ValueError('Invalid job name: ' + name)
        self._name = self._context._jobs.add(self, name)
        
        # finished 
        with self._context.database.scoped_session() as session:
            self._finished = session.sa_session.query(entities.Job).filter_by(name=self.name).one_or_none() is not None
        
    @property
    def name(self):
        '''
        Get unique job name
        
        Contains no invalid file-name characters.
        '''
        return self._name
    
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
        
    @property
    def dependencies(self):
        '''
        Jobs that must be finished before this one can start
        
        Returns
        -------
        {Job}
            Direct dependencies of this job
        '''
        return self._dependencies
    
    def _on_dependencies_changed(self, added, removed):
        # check for circular dependencies (find cycle in dep graph)
        cycles = list(nx.simple_cycles(self._dependency_graph))
        if cycles:
            added = {job.name for job in added}
            cycles = [' -> '.join(job.name for job in cycle) for cycle in cycles]
            raise ValueError('Circular dependencies caused by adding jobs {}. Cycles: {}'.format(added, cycles))
        
    @property
    def _dependency_graph(self):
        graph = nx.DiGraph()
        graph.add_node(self)
        to_visit = {self}
        visited = set()
        while to_visit:
            job = to_visit.pop()
            visited.add(job)
            for dependency in job.dependencies:
                graph.add_edge(job, dependency)
                if dependency not in visited:
                    to_visit.add(dependency)
        return graph
    
    @property
    def finished(self):
        return self._finished
    
    def run(self):
        '''
        Run job and any unfinished dependencies
        
        When a dependency fails, stubbornly waits until the other dependencies
        have finished or failed.
        
        Returns
        -------
        asyncio.Task
            task that runs the job. Task raises `JobFailedError` when the job or
            one of its dependencies fails to finish.
        
        Raises
        ------
        InvalidOperationError
            When the job has already finished.
        ''' #TODO aren't there cases where JobFailedError is raised although it hasn' begun? E.g. failed dep? Maybe raise diff one for that. DependencyFailedError, e.g.
        if self.finished:
            raise InvalidOperationError('Cannot run a finished job')
        if not self._run_task:
            self._run_task = asyncio.ensure_future(self._run())
        return self._run_task
        
    async def _run(self):
        try:
            # Run dependencies
            dependencies = [dependency for dependency in self.dependencies if not dependency.finished]
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
                        raise JobFailedError("Dependency '{}' failed".format(job.name)) from result
            
            # Run self
            try:
                logger.info("Job '{}': submitting".format(self.name))
                await self._server.run(self)
                self._finished = True
                with self._context.database.scoped_session() as session:
                    session.sa_session.add(entities.Job(name=self.name))
                logger.info("Job '{}': finished".format(self.name))
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                logger.info("Job '{}': failed".format(self.name))
                raise JobFailedError('Job failed') from ex
        except asyncio.CancelledError:
            logger.info("Job '{}': cancelled".format(self.name))
            raise
        finally:
            self._run_task = None
        
    def cancel(self):
        '''
        Cancel job and any dependencies it started
        '''
        if self._run_task:
            self._run_task.cancel()
            
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
                    raise JobFailedError('Non-zero exit code: {}'.format(return_code))
    
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
            raise JobFailedError('Job was aborted before it even started running')
        elif result.hasSignal:
            raise JobFailedError('Job was killed with signal {}'.format(result.terminatedSignal))
        elif not result.hasExited:
            raise JobFailedError('Job did not exit normally')
        elif result.hasExited and result.exitStatus != 0:
            raise JobFailedError('Job exited with non-zero exit code: {}'.format(result.exitStatus))
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
        deep_genome.core.cli.AlgorithmMixin or a subclass thereof.
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
