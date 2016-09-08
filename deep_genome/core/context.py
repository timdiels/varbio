# Copyright (C) 2015, 2016 VIB/BEG/UGent - Tim Diels <timdiels.m@gmail.com>
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
Deep Genome context
'''

from chicken_turtle_util import application as app
from deep_genome.core.database import Database

_DatabaseMixin = app.DatabaseMixin(Database)

def Context(version, data_directory, cache_directory):
    '''
    Deep Genome core context, often required by core functions

    Parameters
    ----------
    version
        See chicken_turtle_util.cli.BasicsMixin
    data_directory : Path
        Directory in which to store persistent data
    cache_directory : Path
        Directory to use as cache
        
    See also
    --------
    chicken_turtle_util.application.Context: CLI application context
    '''
    data_directory = data_directory.absolute()
    cache_directory = cache_directory.absolute()
    
    class _Context(_DatabaseMixin, app.BasicsMixin(version), app.Context):
        
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self._persisted_coroutine_functions = {}
            self._jobs = {}
            
        @property
        def data_directory(self):
            '''
            Get data root directory
            
            Only data that needs to be persistent should be stored here.
            '''
            return data_directory
        
        @property
        def cache_directory(self):
            '''
            Get cache root directory
            
            Only non-persistent data that is reused between runs should be stored here.
            '''
            return cache_directory

    return _Context

