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
Test deep_genome.core.pipeline._local.call_repr and related helpers
'''

from deep_genome.core.pipeline import call_repr
from deep_genome.core.pipeline._local import _fully_qualified_name
from chicken_turtle_util.inspect import call_args

#TODO test format_call

def test_fully_qualified_name():
    '''
    Assert for top-level and nested function that: 
    
    - it contains the module name,
    - names of nesting scopes leading up to it (e.g. test_fully_qualified_name in the case below),
    - its own name
    '''
    expected = __name__ + '.test_fully_qualified_name'
    assert _fully_qualified_name(test_fully_qualified_name) == expected
    
    def f():
        pass
    assert _fully_qualified_name(f) == expected + '.<locals>.f'
    
def test_call_repr():
    @call_repr()
    def f(a, b=2, *myargs, call_repr_, x=1, **mykwargs):
        return call_repr_
    
    name = _fully_qualified_name(f)
    assert f(1) == name + '(*args=(), a=1, b=2, x=1)'
    assert f(1, 2, 3, x=10, y=20) == name + '(*args=(3,), a=1, b=2, x=10, y=20)'
    
    @call_repr()
    def f2(b, a, call_repr_):
        return call_repr_
    assert f2(1, 2) == _fully_qualified_name(f2) + '(a=2, b=1)'
    
    f3 = call_repr(exclude_arguments={'a'})(f2.__wrapped__)
    assert f3(1, 2) == _fully_qualified_name(f3) + '(b=1)'
    
    f4 = call_repr(exclude_arguments={'a', 'b'})(f2.__wrapped__)
    assert f4(1, 2) == _fully_qualified_name(f4) + '()'
    
    @call_repr(exclude_arguments={})
    def f5(context, call_repr_):
        return call_repr_
    assert f5(1) == _fully_qualified_name(f5) + '()'
