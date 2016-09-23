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
Stuff to execute on local machine
'''

import asyncio
from chicken_turtle_util import inspect as inspect_, path as path_
from deep_genome.core.pipeline._common import ExitCodeError, format_exit_code_error, fresh_directory
from contextlib import suppress, ExitStack
from functools import wraps
from pathlib import Path
import plumbum as pb
import logging
import inspect

_logger = logging.getLogger(__name__)

def format_call(f, kwargs):
    '''
    Format function call as ``module.func(param=value, ...)``
    
    Parameters
    ----------
    f : function or str
        Name of function in call. If `str`, use  as function name. If function,
        use its fully qualified name.
    kwargs : dict
        Arguments the function is called with as keyword arguments. Values are
        formatted with repr. (Use `call_args` to convert your positional arguments
        to kwargs, see 'See also')
        
    Returns
    -------
    str
        Formatted function call
        
    See also
    --------
    chicken_turtle_util.inspect.call_args : Get function call arguments as a single dict
        
    Notes
    -----
    The fully qualified name is derived from `f.__module__` and `f.__qualname__`.
    '''
    if not isinstance(f, str):
        f = _fully_qualified_name(f)
    kwargs = ', '.join('{}={!r}'.format(key, value) for key, value in sorted(kwargs.items()))
    return '{}({})'.format(f, kwargs)
    
def _fully_qualified_name(f):
    return '{}.{}'.format(f.__module__, f.__qualname__)

#TODO exclude_arguments: we are actually excluding parameters. Args are values, params are ~variables. But people usually mix anyway?
def call_repr(call_repr=None, exclude_arguments=()):
    '''
    Add repr of function call as argument to function 
    
    call_repr, analog to Python's `repr` function, maps function calls uniquely
    to a string representation. Unlike `repr`, it is also deterministic and
    injective. The manner and order in which the arguments are supplied are
    ignored. The format is
    ``{f.__module__}.{f.__qualname__}(arg_name=repr(arg_value), ...)``.
    
    The call repr is supplied to the decorated function as the `call_repr_`
    keyword argument.
    
    Parameters
    ----------
    call_repr : ((f :: function, kwargs :: dict) -> (call_repr :: str)) or None
        If provided, a function which is given the decorated function and all
        arguments as kwargs and returns the the call repr. This parameter is
        mutually exclusive with all other parameters.
        
    exclude_arguments : iterable(str)
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
    >>> @call_repr(exclude_arguments={'a'})
    ... def h(a, b, call_repr_):
    ...     return call_repr_
    ...
    >>> h(1, 2)
    'package.module.h(b=2)'
    
    Optional parameters are always included and the order in which parameters
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
            kwargs['call_repr_'] = _call_repr(f, args, kwargs, call_repr, exclude_arguments)
            return f(*args, **kwargs)
        return decorated
    return decorator

def _call_repr(f, args, kwargs, call_repr=None, exclude_arguments=(), injected_arguments={'call_repr_'}):
    if call_repr and exclude_arguments:
        raise ValueError('call_repr and exclude_args are mutually exclusive.')
    
    argspec = inspect.getfullargspec(f)
    for arg in injected_arguments:
        required =  arg in argspec.args or arg in argspec.kwonlyargs
        if required:
            kwargs[arg] = None
    kwargs = inspect_.call_args(f, args, kwargs)
    for arg in injected_arguments:
        if arg in kwargs:
            del kwargs[arg]
            
    if call_repr:
        return call_repr(f, kwargs)
    else:
        for arg in set(exclude_arguments) | {'context'}:
            if arg in kwargs:
                del kwargs[arg]
        return format_call(f, kwargs)

def persisted(call_repr=None, exclude_arguments=()):
    '''
    Enable caching and persistence on coroutine functions of their awaited results
    
    Caches the returns of a coroutine function's body, taking into account the
    arguments it was given. The cached results are persisted in a database;
    i.e. they are available across application runs.
    
    The coroutine function to decorate must take a deep_genome.core.Context
    instance as argument. The argument must be named `context` and may be a
    positional or keyword argument.
    
    If the coroutine function has a `job_directory` parameter, a job directory
    is created and its path is passed in as `job_directory` as a `pathlib.Path`.
    
    When using `staticmethod` or `classmethod`, be sure to apply `persisted` to
    the inner function, i.e. in this order::
    
        @staticmethod
        @persisted()
        def static(context):
            pass

    Parameters
    ----------
    call_repr : ((f :: function, kwargs :: dict) -> (call_repr :: str)) or None
        If provided, a function which is given the decorated function and all
        arguments as kwargs and returns the the call repr. This parameter is
        mutually exclusive with all other parameters.
        
    exclude_arguments : iterable(str)
        Names of arguments to exclude from the function call repr. The 'context'
        arg is always excluded.
    
    Returns
    -------
    (coroutine function) -> decorated :: coroutine function
        Decorated coroutine function with the same function signature as the
        original.
        
    Notes
    ----- 
    A coroutine function is an ``async def`` or a specific kind of generator
    function (See asyncio.iscoroutinefunction).
    
    A coroutine function returns a coroutine object. You can `await` it or treat
    it like a generator. Our return is a little less than that, it can only be
    awaited. If needed, it could be added, then its fully like a coroutine object.
    
    The decorated coroutine function returns coroutine objects which when
    awaited check the database for a stored result from a previous successful
    run. If any, it is returned, else the original coroutine object is run.
    Starting from Python version 3.5.2, it is a RuntimeError to await on a
    coroutine more than once.
    
    Examples
    --------
    ::
        # mypackage/mymodule.py
        @persisted()   # name defaults to mypackage.mymodule.add
        async def add(context, a, b):
            return a + b
            
        async def main(context):
            assert await add(context, 1, 2) == 3  # when you first run your application, the cache is empty and this await will execute. On subsequent application runs this await's result will be in the cache such that add's body won't be executed again.
            assert await add(context, 1, 2) == 3  # Does not executes add's body as the result is cached
            assert await add(context, 2, 2) == 4  # Does execute add's body (on the first application run) as its arguments are different from what we have in cache
            assert await add(context, 1, 2) == 3  # Previous cache entries aren't forgotten, this returns the result in cache
    '''
    def decorator(f):
        # Note: can't check f with asyncio.iscoroutinefunction as it returns False for staticmethods for example
        if not asyncio.iscoroutinefunction(f):
            raise ValueError('Function to decorate must be a coroutine function')
        
        argspec = inspect.getfullargspec(f)
        if 'context' not in argspec.args and 'context' not in argspec.kwonlyargs and not argspec.varkw:
            raise ValueError('Function to decorate must have an argument named `context`')
            
        @wraps(f)
        async def coroutine_function(*args, **kwargs):
            # get call_repr_ + add to kwargs if requested 
            add_call_repr = 'call_repr_' in argspec.args or 'call_repr_' in argspec.kwonlyargs or argspec.varkw
            add_job_directory = 'job_directory' in argspec.args or 'job_directory' in argspec.kwonlyargs or argspec.varkw
            call_repr_ = _call_repr(f, args, kwargs, call_repr=call_repr, exclude_arguments=exclude_arguments, injected_arguments={'call_repr_', 'job_directory'})
            if add_call_repr:
                kwargs['call_repr_'] = call_repr_
                
            # context
            try:
                context = inspect_.call_args(f, args, kwargs)['context']
            except KeyError:
                raise TypeError(f.__name__ + " missing 1 required argument: 'context'")
            CoroutineCall = context.database.e.CoroutineCall
                 
            # Load/create from database
            with context.database.scoped_session() as session:
                sa_session = session.sa_session
                call = sa_session.query(CoroutineCall).filter_by(name=call_repr_).one_or_none()
                if not call:
                    call = CoroutineCall(name=call_repr_, finished=False)
                    sa_session.add(call)
                    sa_session.flush()
                call_id = call.id
                finished = call.finished
                return_value = call.return_value
                
            # Run if not finished and save result
            if not finished:
                try:
                    _logger.info("Coroutine {} started. Repr: {}".format(call_id, call_repr_))
                    with ExitStack() as stack:
                        if add_job_directory:
                            job_directory = context.pipeline.job_directory('coroutine', call_id)
                            kwargs['job_directory'] = job_directory
                            stack.enter_context(fresh_directory(job_directory))
                        return_value = await f(*args, **kwargs)
                    with context.database.scoped_session() as session:
                        call = session.sa_session.query(context.database.e.CoroutineCall).get(call_id)
                        assert call
                        call.return_value = return_value
                        call.finished = True
                    _logger.info("Coroutine {} finished. Repr: {}".format(call_id, call_repr_))
                except asyncio.CancelledError:
                    _logger.info("Coroutine {} cancelled. Repr: {}".format(call_id, call_repr_))
                    raise
                except Exception:
                    _logger.info("Coroutine {} failed. Repr: {}".format(call_id, call_repr_))
                    raise
            else:
                _logger.debug("Coroutine result fetched from cache, not rerunning: {}".format(call_repr_))
            
            return return_value
        
        return coroutine_function
    return decorator

async def execute(command, directory=Path(), stdout=None, stderr=None):
    '''
    Execute command in directory
    
    When cancelled, the command execution is killed (first tries SIGTERM, after
    a little while uses SIGKILL).
    
    Parameters
    ----------
    command : [any]
        The executable and its arguments as a single list. ``str(command[0])``
        is the executable to execute, ``map(str, command[1:])`` are the args to
        pass it. The executable is looked up using the PATH env var if it's not
        an absolute path.
    directory : pathlib.Path
        Directory in which to execute the command. By default runs in the
        current working directory.
    stdout : Path or file or None
        If Path, stdout is written as file to given path and the file mode is
        set to 0440. If file object, stdout is written to file object. If
        ``None``, `sys.stdout` is used.
    stderr : Path or file or None
        Analog to stdout. If ``None``, `sys.stderr` is used.
        
    Raises
    ------
    ExitCodeError
        If exit code is non-zero
    '''
    with ExitStack() as stack:
        stds = []
        std_files = [None, None]
        for i, std in enumerate((stdout, stderr)):
            if isinstance(std, Path):
                stack.callback(path_.chmod, std, 0o440)
                std_files[i] = std
                std = std.open('w')
                stack.enter_context(std)
            stds.append(std)
        
        command = [str(x) for x in command]
        command[0] = str(pb.local[command[0]].executable)
        process = await asyncio.create_subprocess_exec(*command, cwd=str(directory), stdout=stds[0], stderr=stds[1])
        try:
            return_code = await process.wait()
        except asyncio.CancelledError:
            await _kill(process.pid)
            raise
        if return_code != 0:
            raise ExitCodeError(format_exit_code_error(None, command, return_code, std_files[0], std_files[1]))
    
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
        _logger.warning('Process did not terminate within timeout, sending SIGKILL')
        for process in processes:
            with suppress(psutil.NoSuchProcess):
                process.kill()