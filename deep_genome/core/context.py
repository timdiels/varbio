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

from deep_genome.core.database import Database

class Context(object):
    
    '''
    Deep Genome core context
    
    Provides context (i.e. semi-globals) to many Deep Geneome core functions.

    Parameters
    ----------
    data_directory : Path
        Directory in which to store persistent data
    cache_directory : Path
        Directory to use as cache
    database_credentials : deep_genome.core.database.Credentials
        Passed to :class:`deep_genome.core.database.Database`.
    entities : {class.__name__ => class} or None
        Passed to :class:`deep_genome.core.database.Database`. 
    tables : {name :: str => Table} or None
        Passed to :class:`deep_genome.core.database.Database`.
    '''
    
    def __init__(self, data_directory, cache_directory, database_credentials, entities=None, tables=None):
        self._persisted_coroutine_functions = {}
        self._jobs = {}
        self._data_directory = data_directory.absolute()
        self._cache_directory = cache_directory.absolute()
        self._database = Database(self, database_credentials, entities, tables)
        
    @property
    def database(self):
        return self._database
        
    @property
    def data_directory(self):
        '''
        Get data root directory
        
        Only data that needs to be persistent should be stored here.
        '''
        return self._data_directory
    
    @property
    def cache_directory(self):
        '''
        Get cache root directory
        
        Only non-persistent data that is reused between runs should be stored here.
        '''
        return self._cache_directory
