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
Stuff for both local and drmaa execution
'''

import asyncio
import sys
import attr
import signal
import logging
from ._drmaa import Job
from chicken_turtle_util import logging as logging_

try:
    import drmaa
    _drmaa_import_error = None
except RuntimeError as ex:  # drmaa isn't always used, don't complain immediately when it fails to load
    _drmaa_import_error = ex
    
_logger = logging.getLogger(__name__)

class Pipeline(object):
    
    '''
    Pipeline context class
    
    Do not instantiate directly, use
    :meth:`deep_genome.core.Context.initialise_job` instead
    
    Parameters
    ----------
    context : deep_genome.core.Context
    jobs_directory : pathlib.Path
        Directory in which to create job directories. Job directories are
        provided to DRMAA jobs and @persisted(job_directory=True). They are
        persistent and tied to a job's name (or a coroutine's call_repr).
    max_cores_used : int
        Maximum number of cores used by all running jobs combined. I.e. it
        limits ``sum(job.cores for job in running_jobs)``.
    '''
    
    _instance_counter = 0
    _drmaa_session = None  # `drmaa` cannot have multiple active sessions, so we share one across Pipeline instances
    
    def __init__(self, context, jobs_directory, max_cores_used):
        self._context = context
        self._jobs_directory = jobs_directory
        self._job_resources = JobResources(
            drmaa_session=Pipeline._drmaa_session,
            core_pool=CorePool(max_cores_used),
            job_directory=self.job_directory,
        )
        Pipeline._instance_counter += 1
    
    def dispose(self):
        '''
        Internal, use :meth:`deep_genome.core.Context.dispose` instead.
        
        Dispose pipeline instance, do not use afterwards. It's idempotent.
        '''
        if Pipeline._instance_counter == 1:
            # Do actual clean up
            if Pipeline._drmaa_session:
                Pipeline._drmaa_session.exit()
                Pipeline._drmaa_session = None
        Pipeline._instance_counter -= 1
        
    def drmaa_job(self, name, command, server_arguments=None, version=1, cores=1):
        '''
        Create a DRMAA Job
        
        Parameters
        ----------
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
        
        Returns
        -------
        deep_genome.core.pipeline.Job
        '''
        # Validate `cores`
        if cores > self._job_resources.core_pool.cores:
            raise ValueError(
                'Created job would exceed Pipeline.max_cores_used: {} > {}. '
                'The job would never run. '
                'Either increase max_cores_used or decrease Job.cores.'
                .format(cores, self._job_resources.core_pool.cores)
            )
        
        # Initialise _drmaa_session
        if _drmaa_import_error:
            raise _drmaa_import_error
        if not Pipeline._drmaa_session:
            Pipeline._drmaa_session = drmaa.Session()
            Pipeline._drmaa_session.initialize()
        
        #
        job_resources = attr.assoc(self._job_resources, drmaa_session=Pipeline._drmaa_session)
        return Job(self._context, name, command, job_resources, server_arguments, version, cores)
    
    def job_directory(self, job_type, job_id):
        '''
        Internal, do not use.
        
        Get job directory.
        
        Parameters
        ----------
        job_type : str
        job_id : int
        
        Returns
        ------
        pathlib.Path
            Path to job directory
        '''
        return self._jobs_directory / '{}{}'.format(job_type, job_id)
    
@attr.s(frozen=True)
class JobResources(object):
    '''
    Internal, do not use
    
    Bundle of objects used by a Job (for easier passing around).
    '''
    drmaa_session = attr.ib()
    core_pool = attr.ib()
    
    # see Pipeline.job_directory
    job_directory = attr.ib()
    
class CorePool(object):
    
    '''
    Internal, do not use.
    
    A pool of cores.
    
    Parameters
    ----------
    cores : int
        Number of cores in pool
    '''
    
    def __init__(self, cores):
        self._data = _CorePoolData(cores)
    
    def reserve(self, cores):
        '''
        Temporarily reserve a number of cores
        
        Parameters
        ----------
        cores : int
            Number of cores to reserve. Awaits if requested amount is not
            yet available.
            
        Returns
        -------
        async context manager
        
        Raises
        ------
        ValueError
            More cores requested than total number of cores (including reserved
            cores)
        '''
        return _CoresReservation(cores, self._data)
    
    @property
    def cores(self):
        return self._data.cores
    
@attr.s
class _CorePoolData(object):
    
    _cores = attr.ib()
    reserved = attr.ib(default=0, init=False)
    _condition = attr.ib(default=attr.Factory(asyncio.Condition), init=False)
    
    @property
    def cores(self):
        return self._cores
    
    @property
    def condition(self):
        return self._condition

    @property
    def available(self):
        '''
        Number of cores available (i.e. not reserved)
        '''
        return self.cores - self.reserved
    
class _CoresReservation(object):
    
    '''
    Parameters
    ----------
    cores : int
        Number of cores to reserve
    _CorePoolData
    '''
    
    def __init__(self, cores, core_pool_data):
        self._cores = cores
        self._core_pool_data = core_pool_data
        
    async def __aenter__(self):
        async with self._core_pool_data._condition:
            def predicate():
                return self._core_pool_data.available >= self._cores
            await self._core_pool_data._condition.wait_for(predicate)
            self._core_pool_data.reserved += self._cores

    async def __aexit__(self, exc_type, exc, tb):
        self._core_pool_data.reserved -= self._cores
        async with self._core_pool_data._condition:
            self._core_pool_data._condition.notify_all()
    
def pipeline_cli(main, debug):
    '''
    Run/resume pipeline as CLI front/mid-end
    
    It handles interrupt signals, prints status messages, ...
    
    Fault tolerance:
        
    - When signal interrupted (SIGTERM) or SIGHUP, cancels all jobs and exits
      gracefully. The run can be resumed correctly on a next invocation.
    
    - When killed (SIGKILL), or when server has a power failure, or when
      errors like out-of-memory raised, simply crash. Before resuming you
      should kill any jobs started by the pipeline. Jobs that finished before
      SIGKILL arrived, will not be rerun
    
    - When the awaitable raises an exception, gracefully exits. The run can be
      resumed correctly on a next invocation.
    
    Parameters
    ----------
    main : awaitable
        An awaitable that runs the pipeline when awaited
    debug : bool
        If True, enable debug mode, e.g. more detailed output.
        
    Examples
    --------
    ::
        # main.py
        from deep_genome.core.pipeline import pipeline_cli
        
        async def main():
            print('Hello world')
        
        # Tip: If you distribute your package using setup.py, use
        # `setup(entry_points={'console_scripts': [...]})` instead of the
        # following
        if __name__ == '__main__':
            pipeline_cli(main(), False)
            
    Now on the command line::
    
        $ python main.py
        Hello world
        
        Pipeline: run completed
    '''
    loop = asyncio.get_event_loop()
    task = asyncio.ensure_future(main)
    def cancel():
        _logger.info('Pipeline: cancelling, please wait')
        task.cancel()
    loop.add_signal_handler(signal.SIGHUP, cancel)
    loop.add_signal_handler(signal.SIGINT, cancel)
    loop.add_signal_handler(signal.SIGTERM, cancel)
    
    # Init logging
    stderr_handler, _ = logging_.configure('pipeline.log')
    if debug:
        stderr_handler.setLevel(logging.DEBUG)
    logging.getLogger('deep_genome.core.pipeline').setLevel(logging.DEBUG)
    
    #
    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        _logger.info('Pipeline: cancelled')
        sys.exit(1)
    except Exception:
        _logger.exception('Pipeline: failed: exception was raised:\n\n')
        sys.exit(1)
    finally:
        # Assert no task left hanging
        for task in asyncio.Task.all_tasks():
            assert task.cancelled() or task.done()
            
        # Close loop
        loop.close()
        
    _logger.info('Pipeline: finished')
    