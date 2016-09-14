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
from chicken_turtle_util import path as path_, inspect as inspect_
from chicken_turtle_util.exceptions import InvalidOperationError
from functools import wraps
from ._various import _call_repr, fully_qualified_name
import logging
import inspect

logger = logging.getLogger(__name__)

def persisted(call_repr=None, exclude_args=()): #TODO the changes from moving call_repr from CTU into core are poorly tested
    '''
    Enable caching and persistence on coroutine functions of their awaited results
    
    Caches the returns of a coroutine function's body, taking into account the
    arguments it was given. The cached results are persisted in a database;
    i.e. they are available across application runs.
    
    The coroutine function to decorate must take a deep_genome.core.Context
    instance as argument. The argument must be named `context` and may be a
    positional or keyword argument.
    
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
        arguments as kwargs and returns the repr of the call.
        
    exclude_args : iterable(str)
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
    ''' #TODO fix examples. check example: no longer ignore context by default
    def decorator(f):
        # Note: can't check f with asyncio.iscoroutinefunction as it returns False for staticmethods for example
        if not asyncio.iscoroutinefunction(f):
            raise ValueError('Function to decorate must be a coroutine function')
        
        function_name = fully_qualified_name(f)
            
        argspec = inspect.getfullargspec(f)
        if 'context' not in argspec.args and 'context' not in argspec.kwonlyargs and not argspec.varkw:
            raise ValueError('Function to decorate must have an argument named `context`')
            
        @wraps(f)
        async def coroutine_function(*args, **kwargs):
            # get call_repr_ + add to kwargs if requested 
            add_call_repr = 'call_repr_' in argspec.args or 'call_repr_' in argspec.kwonlyargs or argspec.varkw
            if add_call_repr:
                kwargs['call_repr_'] = None
            call_repr_ = _call_repr(f, args, kwargs, call_repr=call_repr, exclude_args=exclude_args)
            if add_call_repr:
                kwargs['call_repr_'] = call_repr_
                
            # context
            try:
                context = inspect_.call_args(f, args, kwargs)['context']
            except KeyError:
                raise TypeError(f.__name__ + " missing 1 required argument: 'context'")
            CoroutineCall = context.database.e.CoroutineCall
                 
            # Register name context-globally
            if function_name not in context._persisted_coroutine_functions:
                context._persisted_coroutine_functions[function_name] = coroutine_function
            existing = context._persisted_coroutine_functions[function_name]
            if existing != coroutine_function:
                raise ValueError('A persisted coroutine function already exists with this name: name={}, f={}'.format(function_name, existing))
            
            # Load/create from database
            with context.database.scoped_session() as session:
                sa_session = session.sa_session
                call = sa_session.query(CoroutineCall).filter_by(name=call_repr_).one_or_none()
                if not call:
                    call = CoroutineCall(name=call_repr_, finished=False)
                    sa_session.add(call)
                    sa_session.flush()
                id_ = call.id
                finished = call.finished
                return_value = call.return_value
                
            # Run if not finished and save result
            if not finished:
                try:
                    logger.info("Coroutine {} started. Repr: {}".format(id_, call_repr_))
                    return_value = await f(*args, **kwargs)
                    finished = True
                    with context.database.scoped_session() as session:
                        call = session.sa_session.query(CoroutineCall).get(id_)
                        assert call
                        call.return_value = return_value
                        call.finished = True
                    logger.info("Coroutine {} finished. Repr: {}".format(id_, call_repr_))
                except asyncio.CancelledError:
                    logger.info("Coroutine {} cancelled. Repr: {}".format(id_, call_repr_))
                    raise
                except Exception:
                    logger.info("Coroutine {} failed. Repr: {}".format(id_, call_repr_))
                    raise
            else:
                logger.debug("Coroutine result fetched from cache, not rerunning: {}".format(call_repr_))
            
            return return_value
        
        return coroutine_function
    return decorator
