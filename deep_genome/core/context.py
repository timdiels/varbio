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

from chicken_turtle_util.exceptions import InvalidOperationError
from deep_genome.core.database import Database
from deep_genome.core.pipeline import Pipeline

class Context(object):
    
    '''
    Deep Genome core context
    
    Provides context (i.e. semi-globals) to many Deep Geneome core functions.
    Usually you want to construct just one and pass it to all functions that
    need it.

    Parameters
    ----------
    database_credentials : deep_genome.core.database.Credentials
        Passed to :class:`deep_genome.core.database.Database`.
    entities : {class.__name__ => class} or None
        Passed to :class:`deep_genome.core.database.Database`. 
    tables : {name :: str => Table} or None
        Passed to :class:`deep_genome.core.database.Database`.
    '''
    
    def __init__(self, database_credentials, entities=None, tables=None):
        self._database = Database(self, database_credentials, entities, tables)
        self._pipeline = None
        self._disposed = False
        
    @property
    def database(self):
        return self._database
        
    def initialise_pipeline(self, jobs_directory):
        '''
        Initialise self.pipeline attribute
        
        Parameters
        ----------
        jobs_directory : pathlib.Path
            Directory in which to create job directories. Job directories are
            provided to DRMAA jobs and @persisted(job_directory=True). They are
            persistent and tied to a job's name (or a coroutine's call_repr).
        '''
        self._pipeline = Pipeline(self, jobs_directory) 
        
    @property
    def pipeline(self):
        '''
        Get pipeline context
        
        Call `initialise_pipeline` before using this attribute.
        
        Returns
        -------
        deep_genome.core.pipeline.Pipeline
        '''
        if not self._pipeline:
            raise InvalidOperationError('Pipeline not initialised. Call context.initialise_pipeline first.')
        return self._pipeline
    
    def dispose(self):
        '''
        Release any resources
        
        The context instance should not be used after this call, though multiple
        calls to dispose are allowed
        '''
        if self._disposed:
            return
        if self._pipeline:
            self._pipeline.dispose()
        self._disposed = True
