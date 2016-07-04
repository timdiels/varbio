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
import traceback
import sys
import signal
from chicken_turtle_util import inspect as inspect_

def pipeline_cli(main, Context):
    
    '''
    Get CLI frontend to running a pipeline
    
    It takes care of details such as handling interrupt signals.
    
    Parameters
    ----------
    main : async (Context)
        Main coroutine function of the pipeline that is called with the
        application context you provided.
    Context
        Application context class to use. Should be
        deep_genome.core.context.AlgorithmMixin or a subclass thereof.
        
    Returns
    -------
    click.Command
    
    Examples
    --------
    ::
        # main.py
        from deep_genome.core import AlgorithmContext
        from deep_genome.core.pipeline import pipeline_cli
        
        version = '1.0.0'
        
        class MyContext(AlgorithmContext(version):
            pass  # any application context things specific to your application. If none, just use AlgorithmContext directly
            
        async def _main(context):
            print('Hello world')
            
        main = pipeline_cli(_main, MyContext)
        
        # Tip: If you distribute your package using setup.py, use
        # `setup(entry_points={'console_scripts': [...]})` instead of the
        # following
        if __name__ == '__main__':
            main()
            
    Now on the command line::
    
        $ python main.py
        Hello world
        Pipeline: run completed
    '''
    
    @Context.command()
    def _main(context):
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
        task = asyncio.ensure_future(main(context))
        loop.add_signal_handler(signal.SIGTERM, task.cancel)
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
    return _main

def call_repr(*args, **kwargs):
    '''
    Like `chicken_turtle_util.inspect.call_repr`, except `exclude_args` always
    includes 'context'.
    '''
    kwargs['exclude_args'] = set(kwargs.get('exclude_args', {})) | {'context'}
    return inspect_.call_repr(*args, **kwargs)
    