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
from chicken_turtle_util.configuration import ConfigurationLoader
from deep_genome.core.configuration import Configuration
from deep_genome.core.database import Database

_DatabaseMixin = app.DatabaseMixin(Database)

# ConfigurationMixin
_loader = ConfigurationLoader('deep_genome.core', 'deep_genome', 'core')     
    
def Context(version, data_directory, cache_directory, configurations={}):
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
    configurations : {configuration_name :: str => help_message :: str}
        Additional configuration files. 'core' as configuration name is reserved
        by DG core.
        
    See also
    --------
    chicken_turtle_util.application.Context: CLI application context
    '''
    if 'core' in configurations:
        raise ValueError('Configuration name "core" is reserved to Deep Genome core')
    
    configurations = configurations.copy()
    configurations['core'] = _loader.cli_help_message('Configure advanced options such as how exceptional cases should be handled.')
    _ConfigurationsMixin = app.ConfigurationsMixin(configurations)
    
    data_directory = data_directory.absolute()
    cache_directory = cache_directory.absolute()
    
    class _Context(_ConfigurationsMixin, _DatabaseMixin, app.BasicsMixin(version), app.Context):
        
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self._persisted_coroutine_functions = {}
            self._jobs = {}
            
            self.__configuration = Configuration(_loader.load(self._configuration_paths.get('core')))
            
        @property
        def configuration(self):
            '''
            Get DG core configuration
            
            Returns
            -------
            deep_genome.core.configuration.Configuration
            '''
            return self.__configuration
        
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

