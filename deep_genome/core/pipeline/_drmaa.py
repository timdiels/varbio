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
Things to execute on DRMAA cluster
'''

import asyncio
import logging
from pathlib import Path
import plumbum as pb
from contextlib import suppress
from deep_genome.core.pipeline._common import ExitCodeError, format_exit_code_error, fresh_directory

try:
    import drmaa
    _drmaa_import_error = None
except RuntimeError as ex:  # drmaa isn't always used, don't complain immediately when it fails to load
    _drmaa_import_error = ex

_logger = logging.getLogger(__name__)
    
class Job(object):
    
    '''
    A job with a command that can be submitted to a job server for execution
    
    The job can be run by awaiting it. It can be awaited at most once.
    
    Do not instantiate directly, use
    :meth:`deep_genome.core.pipeline.Pipeline.drmaa_job` instead.
    
    Parameters
    ----------
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
                
    command : [any]
        ``str(command[0])`` is the executable (script with shebang or binary)
        to execute, ``map(str, command[1:])`` are the args to pass it. The
        executable is looked up using the PATH env var if it's not absolute.
    server_arguments : str
        Additional arguments specific to the job server. E.g. in DRMAAJobServer
        this corresponds to the `native specification`_, which in the case of
        SGE or OGS is a string of options given to qsub (according to
        http://linux.die.net/man/3/drmaa_attributes).
        
    .. _native specification: http://gridscheduler.sourceforge.net/javadocs/org/ggf/drmaa/JobTemplate.html#setNativeSpecification(java.lang.String)
    '''
    #TODO old docstring
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
    
    # Note: If you get "drmaa.errors.DeniedByDrmException: code 17: error: no
    # suitable queues" consider prefixing the server_args with '-w no'. This
    # most often happens when specifying mem_free on clusters that aren't
    # configured for it (see 
    # http://gridscheduler.sourceforge.net/howto/commonproblems.html#interactive)
    
    def __init__(self, context, drmaa_session, name, command, server_arguments=None):
        if _drmaa_import_error:
            raise _drmaa_import_error
        
        self._context = context
        self._drmaa_session = drmaa_session
        self._name = name
        command = [str(x) for x in command]
        self._executable = Path(str(pb.local[command[0]].executable))
        self._arguments = command[1:]
        self._server_arguments = server_arguments
        
        # Load/create from database 
        Job = self._context.database.e.Job
        with self._context.database.scoped_session() as session:
            sa_session = session.sa_session
            job = sa_session.query(Job).filter_by(name=self._name).one_or_none()
            if not job:
                job = Job(name=name, finished=False)
                sa_session.add(job)
                sa_session.flush()
            self._id = job.id
            self._finished = job.finished
            
        #
        self._job_directory = self._context.pipeline.job_directory('job', self._id)
        
    async def run(self):
        '''
        Run job on server
        
        If job has already been run successfully before, return immediately.
        '''
        if self._finished:
            _logger.debug("Job {} already finished, not rerunning. Name: {}".format(self._id, self._name))
            return
        try:
            _logger.info("Job {} started. Name: {}".format(self._id, self._name))
            await self._run()
            self._finished = True
            with self._context.database.scoped_session() as session:
                job = session.sa_session.query(self._context.database.e.Job).get(self._id)
                assert job
                job.finished = True
            _logger.info("Job {} finished. Name: {}".format(self._id, self._name))
        except asyncio.CancelledError:
            _logger.info("Job {} cancelled. Name: {}".format(self._id, self._name))
            raise
        except Exception:
            _logger.info("Job {} failed. Name: {}".format(self._id, self._name))
            raise
        
    async def _run(self):
        loop = asyncio.get_event_loop()
        
        with fresh_directory(self._job_directory):
            self.directory.mkdir()
            
            # Submit job
            job_template = self._drmaa_session.createJobTemplate()
            try:
                job_template.workingDirectory = str(self.directory)
                job_template.outputPath = ':' + str(self.stdout_file)
                job_template.errorPath = ':' + str(self.stderr_file)
                job_template.remoteCommand = str(self._executable)
                job_template.args = self._arguments
                if self._server_arguments:
                    job_template.nativeSpecification = self._server_arguments
                job_id = await loop.run_in_executor(None, self._drmaa_session.runJob, job_template)
            finally:
                self._drmaa_session.deleteJobTemplate(job_template)
                
            # Wait for job
            try:
                result = await loop.run_in_executor(None, self._drmaa_session.wait, job_id, drmaa.Session.TIMEOUT_WAIT_FOREVER)
            except asyncio.CancelledError as ex:
                with suppress(drmaa.errors.InvalidJobException):  # perhaps job_id has already disappeared from server (because it finished or was terminated?)
                    await loop.run_in_executor(None, self._drmaa_session.control, job_id, drmaa.JobControlAction.TERMINATE)  # May return before job is actually terminated
                    await loop.run_in_executor(None, self._drmaa_session.wait, job_id, drmaa.Session.TIMEOUT_WAIT_FOREVER)  # So try to wait for termination
                raise ex
        
        # Check result
        if result.wasAborted:
            raise Exception('Job {} was aborted before it even started running'.format(self._id))
        elif result.hasSignal:
            raise Exception('Job {} was killed with signal {}'.format(self._id, result.terminatedSignal))
        elif not result.hasExited:
            raise Exception('Job {} did not exit normally'.format(self._id))
        elif result.hasExited and result.exitStatus != 0:
            raise ExitCodeError(format_exit_code_error(
                'Job {}'.format(self._id),
                [self._executable] + list(self._arguments),
                result.exitStatus,
                self.stdout_file,
                self.stderr_file
            ))
        _logger.debug("Job {}'s resource usage was: {}".format(self._id, result.resourceUsage))
        
    @property
    def directory(self):
        '''
        Get working directory
        
        Returns
        -------
        pathlib.Path
        '''
        return self._job_directory / 'output'
        
    @property
    def stderr_file(self):
        '''
        Get path to stderr output file
        
        Returns
        -------
        pathlib.Path
        '''
        return self._job_directory / 'stderr'
    
    @property
    def stdout_file(self):
        '''
        Get path to stdout output file
        
        Returns
        -------
        pathlib.Path
        '''
        return self._job_directory / 'stdout'
            
    def __str__(self):
        max_ = 100
        if len(self._name) > max_:
            name = self._name[:max_] + '...'
        else:
            name = self._name
        return 'Job({!r}, {!r})'.format(self._id, name)
    
    def __repr__(self):
        return 'Job({!r})'.format(self._id)
