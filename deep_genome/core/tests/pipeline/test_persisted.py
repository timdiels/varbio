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
Test deep_genome.core.pipeline._persisted
'''

from deep_genome.core.pipeline import persisted
from chicken_turtle_util.inspect import get_call_repr
from .common import assert_task_log
import inspect
import asyncio
import pytest
        
class TestContextFinding(object):
    '''
    Test whether persisted can always find the context argument
    '''
    
    # XXX test that bad funcs and bad calls, cause errors
    '''
    both for funcs and classes with (regular and) staticmethod and classmethod
    def f(context) # not a coroutine
    async def g()  # no context arg
    async def h(**kwargs): # okay
        pass
    h()  # no context arg
    h(context=context)  # okay
    '''
    
    @pytest.mark.asyncio
    async def test_functions(self, context):
        @persisted()
        async def f(context):
            pass
        await f(context) # Note: persisted simply raises if it doesn't find the context, after all it needs it, it has to use it
        
        @persisted()
        async def g(arg, arg2, context, arg3):
            pass
        await g(1, 2, context, 3)
        await g(1, 2, context=context, arg3=3)
        
        @persisted()
        async def h(arg, arg2, context=None, arg3=None):
            pass
        await h(1, 2, context, 3)
        await h(1, 2, context)
        await h(1, 2, arg3=3, context=context)
        
        @persisted()
        async def f2(arg, arg2, context=context, arg3=None):
            pass
        await f2(1, 2)
        await f2(1, 2, arg3=3)
        
        @persisted()
        async def f3(arg, *args, context=context, arg3=None):
            pass
        await f3(1, 2)
        
        @persisted()
        async def f4(arg, *args, arg3=None, context=context):
            pass
        await f4(1, 2)
        
        @persisted()
        async def f5(arg, **kwargs):
            pass
        await f5(1, context=context)
    
    @pytest.mark.asyncio    
    async def test_methods(self, context):
        class A(object):
            @persisted()
            async def method(self, arg, context, arg3):
                pass
            
            @staticmethod
            @persisted()
            async def static_method(arg, context, arg3):
                pass
            
            @classmethod
            @persisted()
            async def class_method(cls, arg, context, arg3):
                pass
        
        a = A()    
        await a.method(1, context, 3)
        await a.method(1, context=context, arg3=3)
        await a.static_method(1, context, 3)
        await a.static_method(1, context=context, arg3=3)
        await a.class_method(1, context, 3)
        await a.class_method(1, context=context, arg3=3)
        
class TestIgnoreArgs(object):
    
    @pytest.mark.asyncio
    async def test_regular_arg(self, context):
        @persisted(exclude_args=['b'])
        async def f(a, b, context):
            return b
        assert await f(1, 2, context) == 2
        assert await f(1, 1, context) == 2
        assert await f(2, 1, context) == 1
    
    @pytest.mark.asyncio
    async def test_kwonly(self, context):
        @persisted(exclude_args=['b'])
        async def f(a, *args, b=1, context):
            return b
        assert await f(1, context=context, b=2) == 2
        assert await f(1, context=context, b=1) == 2
        assert await f(2, context=context, b=1) == 1
    
    @pytest.mark.asyncio
    async def test_in_kwargs(self, context):
        @persisted(exclude_args={'b'})
        async def f(a, context, **kwargs):
            return kwargs['b']
        assert await f(1, context, b=2) == 2
        assert await f(1, context, b=1) == 2
        assert await f(2, context, b=1) == 1
        
    @pytest.mark.asyncio
    async def test_no_nameclash(self, context):
        @persisted(exclude_args={'args'})
        async def f1(context, *args, **kwargs):
            return kwargs['args']
        assert await f1(context, args=2) == 2
        assert await f1(context, args=1) == 2
        assert await f1(context, args=1, other=1) == 1
        
        @persisted(exclude_args={'kwargs'})
        async def f2(context, **kwargs):
            return kwargs['kwargs']
        assert await f2(context, kwargs=2) == 2
        assert await f2(context, kwargs=1) == 2
        assert await f2(context, kwargs=1, other=1) == 1
        
    @pytest.mark.asyncio
    async def test_multiple(self, context):
        @persisted(exclude_args=('a', 'c'))
        async def f(a, b, c, context):
            return a + c
        assert await f(2, 1, 3, context) == 5
        assert await f(0, 1, 1, context) == 5
        assert await f(0, 2, 1, context) == 1
        
class CoroutineMock(object):
    
    def __init__(self, caplog):
        self._action = 'succeed'
        self._caplog = caplog
        
    @persisted(exclude_args={'self'})
    async def f(self, context, x):
        if self._action == 'succeed':
            return x
        elif self._action == 'fail':
            raise Exception('fail')
        elif self._action == 'forever':
            await asyncio.sleep(9999999)
        else:
            assert False
    
    @property
    def action(self):
        pass
    
    @action.setter
    def action(self, action):
        self._action = action
        
    def assert_log(self, events, x):
        return assert_task_log(self._caplog, 'Coroutine', get_call_repr(self.__class__.f.__wrapped__, [self, None, x], exclude_args={'context', 'self'}), events)
        
@pytest.fixture
def coroutine_mock(caplog):
    return CoroutineMock(caplog)

@pytest.mark.asyncio
async def test_succeed(coroutine_mock, context, context2, caplog):
    # when cache miss, run
    with coroutine_mock.assert_log(['started', 'finished'], 1):
        awaitable = coroutine_mock.f(context, 1)
        assert inspect.isawaitable(awaitable)
        assert await awaitable == 1
    
    # when finished, don't rerun (cache)
    with coroutine_mock.assert_log([], 1):
        assert await coroutine_mock.f(context, 1) == 1
    
    # not even across runs (persist)
    with coroutine_mock.assert_log([], 1):
        assert await coroutine_mock.f(context2, 1) == 1
        
    # but do run when args are different
    with coroutine_mock.assert_log(['started', 'finished'], 2):
        assert await coroutine_mock.f(context, 2) == 2
    
    # without forgetting the original
    with coroutine_mock.assert_log([], 1):
        assert await coroutine_mock.f(context, 1) == 1
    
@pytest.mark.asyncio
async def test_fail(coroutine_mock, context):
    # When coroutine fails, it raises
    coroutine_mock.action = 'fail'
    with coroutine_mock.assert_log(['started', 'failed'], 1):
        with pytest.raises(Exception):
            await coroutine_mock.f(context, 1)
    
    # When coroutine recovers, it runs and finishes fine
    coroutine_mock.action = 'succeed'
    with coroutine_mock.assert_log(['started', 'finished'], 1):
        assert await coroutine_mock.f(context, 1) == 1
    
@pytest.mark.asyncio
async def test_cancel(coroutine_mock, context):
    # When coroutine cancelled, it raises asyncio.CancelledError
    coroutine_mock.action = 'forever'
    task = asyncio.ensure_future(coroutine_mock.f(context, 1))
    asyncio.get_event_loop().call_later(3, task.cancel)
    with coroutine_mock.assert_log(['started', 'cancelled'], 1):
        with pytest.raises(asyncio.CancelledError):
            await task
    
    # When it recovers, it finishes fine
    coroutine_mock.action = 'succeed'
    with coroutine_mock.assert_log(['started', 'finished'], 1):
        assert await coroutine_mock.f(context, 1) == 1
