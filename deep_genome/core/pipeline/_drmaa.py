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
Stuff to execute on DRMAA cluster
'''

import asyncio
import logging
from pathlib import Path
import plumbum as pb
from contextlib import suppress
from deep_genome.core.pipeline._common import ExitCodeError, format_exit_code_error, fresh_directory
import pprint

try:
    import drmaa
    _drmaa_import_error = None
except RuntimeError as ex:  # drmaa isn't always used, don't complain immediately when it fails to load
    _drmaa_import_error = ex

_logger = logging.getLogger(__name__)
    
class Job(object):
    
    '''
    A command to submit and run on a DRMAA server
    
    Do not instantiate directly, use
    :meth:`deep_genome.core.pipeline.Pipeline.drmaa_job` instead.
    
    Parameters
    ----------
    context : deep_genome.core.Context
    drmaa_session : drmaa.Session
    name : str
        Unique job name. May use any characters, even whitespace (and nul
        characters), but it's recommended to keep it somewhat readable. It's
        best to use the :func:`deep_genome.core.pipeline.call_repr` of the
        surrounding call::
        
            @call_repr()
            async def mass_multiply(context, arg1, arg2, call_repr_):
                job = context.pipeline.drmaa_job(call_repr_, server, ['command', arg1, arg2])
                await job.run()
                
        Avoid using counters to make names unique, this fails when the function
        is called in a different order in a next run, assigning different
        numbers.
                
    command : [any]
        The executable and its arguments as a single list. ``str(command[0])``
        is the executable to execute, ``map(str, command[1:])`` are the args to
        pass it. The executable is looked up using the PATH env var if it's not
        an absolute path.
    core_pool : deep_genome.core.pipeline._various.CorePool
    server_arguments : str or None
        A DRMAA native specification, which in the case of SGE or OGS is a
        string of options given to qsub (see also
        http://linux.die.net/man/3/drmaa_attributes). When requesting more than
        1 core, remember to also set `cores`.
    version : int
        Version number of the command. Cached results from other versions are
        ignored. I.e. when the job is run after a version change, it will rerun
        and overwrite the result of a different version (if any) in the cache.
    cores : int
        Number of cores used. To actually request them, use server_arguments.
        This is required for adhering to ``Pipeline(max_total_cores)``.
    '''
    
    # Note: If you get "drmaa.errors.DeniedByDrmException: code 17: error: no
    # suitable queues" consider prefixing the server_args with '-w no'. This
    # most often happens when specifying mem_free on clusters that aren't
    # configured for it (see 
    # http://gridscheduler.sourceforge.net/howto/commonproblems.html#interactive)
    
    def __init__(self, context, drmaa_session, name, command, core_pool, server_arguments=None, version=1, cores=1):
        if _drmaa_import_error:
            raise _drmaa_import_error
        
        self._context = context
        self._drmaa_session = drmaa_session
        self._name = name
        command = [str(x) for x in command]
        self._executable = Path(str(pb.local[command[0]].executable))
        self._arguments = command[1:]
        self._server_arguments = server_arguments
        self._version = version
        self._cores = cores
        self._core_pool = core_pool
        
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
            self._finished = job.finished == version
            
        #
        self._job_directory = self._context.pipeline.job_directory('job', self._id)
        
    @property
    def _command(self):
        return [self._executable] + list(self._arguments)
    
    async def run(self):
        '''
        Run job on server
        
        If job has already been run successfully before, return immediately.
        '''
        _logger.debug("{}: name={!r}".format(self, self._name))
        if self._finished:
            _logger.info("{}: fetched from cache.".format(self))
            return
        try:
            async with self._core_pool.reserve(self.cores):
                _logger.info("{}: started:\n{}".format(self, ' '.join(map(repr, self._command))))
                await self._run()
            self._finished = True
            with self._context.database.scoped_session() as session:
                job = session.sa_session.query(self._context.database.e.Job).get(self._id)
                assert job
                job.finished = self._version
            _logger.info("{}: finished.".format(self))
        except asyncio.CancelledError:
            _logger.info("{}: cancelled.".format(self))
            raise
        except Exception:
            _logger.error("{}: failed.".format(self))
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
                _logger.debug('{}: drmaa job id = {}'.format(self, job_id))
                result = await loop.run_in_executor(None, self._drmaa_session.wait, job_id, drmaa.Session.TIMEOUT_WAIT_FOREVER)
            except asyncio.CancelledError as ex:
                _logger.info("{}: cancelling (drmaa job id = {}).".format(self, job_id))
                with suppress(drmaa.errors.InvalidJobException):  # perhaps job_id has already disappeared from server (because it finished or was terminated?)
                    await loop.run_in_executor(None, self._drmaa_session.control, job_id, drmaa.JobControlAction.TERMINATE)  # May return before job is actually terminated
                    await loop.run_in_executor(None, self._drmaa_session.wait, job_id, drmaa.Session.TIMEOUT_WAIT_FOREVER)  # So try to wait for termination
                raise ex
        
        # Check result
        if result.wasAborted:
            raise Exception(
                '{} was aborted before it even started running. '
                'Perhaps the configured job directory is inaccessible to the cluster?'
                .format(self)
            )
        elif result.hasSignal:
            raise Exception('{} was killed with signal {}'.format(self, result.terminatedSignal))
        elif not result.hasExited:
            raise Exception('{} did not exit normally'.format(self))  # not sure whether this can actually happen
        elif result.hasExited and result.exitStatus != 0:
            raise ExitCodeError(format_exit_code_error(
                str(self),
                self._command,
                result.exitStatus,
                self.stdout_file,
                self.stderr_file
            ))
        _logger.debug("{}: resource usage was:\n{}".format(self, pprint.pformat(result.resourceUsage)))
        
    @property
    def directory(self):
        '''
        Working directory
        
        This is the cwd in which the command runs/ran.
        
        Returns
        -------
        pathlib.Path
        '''
        return self._job_directory / 'output'
        
    @property
    def stderr_file(self):
        '''
        Path to stderr output file
        
        Returns
        -------
        pathlib.Path
        '''
        return self._job_directory / 'stderr'
    
    @property
    def stdout_file(self):
        '''
        Path to stdout output file
        
        Returns
        -------
        pathlib.Path
        '''
        return self._job_directory / 'stdout'
    
    @property
    def cores(self):
        '''
        Number of cores used when running.
        
        Returns
        -------
        int
        '''
        return self._cores
            
    def __repr__(self):
        return 'Job({!r})'.format(self._id)
    
    def __str__(self):
        return 'drmaa_job[{!r}]'.format(self._id)
