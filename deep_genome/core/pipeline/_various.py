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

from chicken_turtle_util import inspect as inspect_
from functools import wraps
import asyncio
import traceback
import sys
import signal
import logging
import click

def pipeline_cli(main, version):
    
    '''
    Get CLI frontend to running a pipeline
    
    It takes care of details such as handling interrupt signals.
    
    Parameters
    ----------
    main : async () -> None
        Main coroutine function of the pipeline to call.
    version : str
        Pipeline version, e.g. ``1.0.0``.
        
    Returns
    -------
    click.Command
    
    Examples
    --------
    ::
        # main.py
        from deep_genome.core.pipeline import pipeline_cli
        
        version = '1.0.0'
        
        async def _main():
            print('Hello world')
            
        main = pipeline_cli(_main, version)
        
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
    
    #TODO maybe one small test to check:
    # --debug, and also check for log file in both cases
    #
    # and another for (parametrize -h, --help; assert output string matches exactly some manually checked str):
    # --version (correct version printed too)
    # -h, --help
    @click.command(context_settings={'help_option_names': ['-h', '--help']})
    @click.version_option(version=version)
    @click.option('--debug/--no-debug', default=False, help='Verbose debug output')
    def _main(debug):
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
        task = asyncio.ensure_future(main())
        loop.add_signal_handler(signal.SIGTERM, task.cancel)
        
        # Note: keep stdout/stderr readable, even when debugging. Full unambiguous details are sent to a log file
        if debug:
            level = logging.DEBUG
        else:
            level = logging.INFO
        logging.basicConfig(level=level, format='{levelname[0]}: {message}', style='{')  # log info to stdout in terse format
        root_logger = logging.getLogger()  # log debug and higher to file
        file_handler = logging.FileHandler('pipeline.log')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter('{levelname[0]} {asctime} {name} ({module}:{lineno}): {message}', style='{'))
        root_logger.addHandler(file_handler)
        
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

def call_repr(call_repr=None, exclude_args=()): #TODO update the example
    '''
    Add repr of function call as argument to function 
    
    Function calls are uniquely mapped to strings (it's deterministic and
    injective) and supplied to the decorated function as the `call_repr_`
    keyword argument. The manner and order in which the arguments are supplied
    are ignored. The format is
    ``{f.__module__}.{f.__qualname__}(arg_name=repr(arg_value), ...)``.
    
    Parameters
    ----------
    call_repr : ((f :: function, kwargs :: dict) -> (call_repr :: str)) or None
        If provided, a function which is given the decorated function and all
        arguments as kwargs and returns the repr of the call.
        
    exclude_args : iterable(str)
        Names of arguments to exclude from the function call repr. The 'context'
        arg is always excluded.
        
    Returns
    -------
    function -> decorated_function
        Function which decorates functions with a `call_repr_` argument.
    
    Examples
    --------
    >>> # package/module.py
    >>> @call_repr()
    ... def f(a, b=2, *myargs, call_repr_, x=1, **mykwargs):
    ...     return call_repr_
    ...
    >>> f(1)
    'package.module.f(*args=(), a=1, b=2, x=1)'
    >>> f(1, 2, 3, x=10, y=20)
    'package.module.f(*args=(1,), a=1, b=2, x=10, y=20)'
    >>> @call_repr(name='my.func')
    ... def g(call_repr_):
    ...     return call_repr_
    ...
    >>> g()
    'my.func()'
    >>> @call_repr(exclude_args={'a'})
    ... def h(a, b, call_repr_):
    ...     return call_repr_
    ...
    >>> h(1, 2)
    'package.module.h(b=2)'
    
    With parametrised nesting you may want to:
    
    >>> @call_repr()
    ... def f(a, b, call_repr_):
    ...     @call_repr(name=call_repr_ + '::g')
    ...     def g(x, call_repr_):
    ...         return call_repr_
    ...
    >>> f(1,2)('x')
    "package.module.f(a=1, b=2)::g(x='x')"
    
    Optional arguments are always included and the order in which arguments
    appear in the function definition is ignored:
    
    >>> @call_repr()
    ... def f(b, a=None, call_repr_):
    ...     return call_repr_
    ...
    >>> f(1)
    'package.module.f(a=None, b=1)'
    '''
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            kwargs['call_repr_'] = None  # otherwise call_args fails on functions with call_repr_ as required arg
            kwargs['call_repr_'] = _call_repr(f, args, kwargs, call_repr, exclude_args)
            return f(*args, **kwargs)
        return decorated
    return decorator

def _call_repr(f, args, kwargs, call_repr=None, exclude_args=()):
    if call_repr and exclude_args:
        raise ValueError('call_repr and exclude_args are mutually exclusive.')
    kwargs = inspect_.call_args(f, args, kwargs)
    if 'call_repr_' in kwargs:
        del kwargs['call_repr_']
    if call_repr:
        return call_repr(f, kwargs)
    else:
        for arg in set(exclude_args) | {'context'}:
            if arg in kwargs:
                del kwargs[arg]
        return format_call(f, kwargs)

def format_call(f, kwargs):
    '''
    Format function call as ``module.func(param=value, ...)``
    
    Parameters
    ----------
    f : function or str
        Name of function in call. If str, use str as function name. If function, use its fully qualified name.
    kwargs : dict
        Arguments the function is called with. Values are formatted with repr.
        
    Returns
    -------
    str
        Formatted function call
    '''
    if not isinstance(f, str):
        f = inspect_.fully_qualified_name(f)
    kwargs = ', '.join('{}={!r}'.format(key, value) for key, value in sorted(kwargs.items()))
    return '{}({})'.format(f, kwargs)
    