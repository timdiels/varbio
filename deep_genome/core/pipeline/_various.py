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
import traceback
import sys
import signal
import logging
from ._drmaa import Job

try:
    import drmaa
    _drmaa_import_error = None
except RuntimeError as ex:  # drmaa isn't always used, don't complain immediately when it fails to load
    _drmaa_import_error = ex

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
    '''
    
    _instance_counter = 0
    _drmaa_session = None  # `drmaa` cannot have multiple active sessions, so we share one across Pipeline instances
    
    def __init__(self, context, jobs_directory):
        self._context = context
        self._jobs_directory = jobs_directory
        Pipeline._instance_counter += 1
    
    def dispose(self): # TODO keep internal, to be called by Context.dispose
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
        
    def drmaa_job(self, name, command, server_arguments=None, version=1):
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
            http://linux.die.net/man/3/drmaa_attributes).
        version : int
            Version number of the command. Cached results from other versions are
            ignored. I.e. when the job is run after a version change, it will rerun
            and overwrite the result of a different version (if any) in the cache.
        
        Returns
        -------
        deep_genome.core.pipeline.Job
        '''
        # Initialise _drmaa_session
        if _drmaa_import_error:
            raise _drmaa_import_error
        if not Pipeline._drmaa_session:
            Pipeline._drmaa_session = drmaa.Session()
            Pipeline._drmaa_session.initialize()
        
        #
        return Job(self._context, Pipeline._drmaa_session, name, command, server_arguments, version)
    
    def job_directory(self, job_type, job_id):
        '''
        Internal: Get job directory.
        
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
    
def pipeline_cli(main, debug):
    '''
    Run/resume pipeline as CLI front/mid-end
    
    It handles interrupt signals, prints status messages, ...
    
    Fault tolerance:
        
    - When signal interrupted (SIGTERM), stops all jobs and gracefully exits.
      The run can be resumed correctly on a next invocation.
    
    - When killed (SIGKILL), or when server has power failure, or when
      errors like out-of-memory raised, simply crash. Before resuming you
      should kill any jobs started by the pipeline. Jobs that finished before
      SIGKILL arrived, will not be rerun
    
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
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    
    # Note: do not use logging.basicConfig as it does not play along with caplog in testing
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # handlers will cut down on this
    
    # log info or debug to stderr in terse format (details go in log file, not stderr)
    stderr_handler = logging.StreamHandler() # to stderr
    stderr_handler.setLevel(logging.DEBUG if debug else logging.INFO)
    stderr_handler.setFormatter(logging.Formatter('{levelname[0]}: {message}', style='{'))
    root_logger.addHandler(stderr_handler)
    
    # log debug and higher to file in long format
    file_handler = logging.FileHandler('pipeline.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter('{levelname[0]} {asctime} {name} ({module}:{lineno}):\n{message}\n', style='{'))
    root_logger.addHandler(file_handler)
    
    #
    try:
        loop.run_until_complete(task)
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