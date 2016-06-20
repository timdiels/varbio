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
from deep_genome.core.pipeline import Tasks

_DatabaseMixin = app.DatabaseMixin(Database)

# ConfigurationMixin
_loader = ConfigurationLoader('deep_genome.core', 'deep_genome', 'core')    

#
_DataDirectoryMixin = app.DataDirectoryMixin('deep_genome')
_CacheDirectoryMixin = app.CacheDirectoryMixin('deep_genome')

# TODO include in AlgorithmMixin and do properly
# from deep_genome.core.cache import Cache
# class _CacheMixin(_DatabaseMixin, _CacheDirectoryMixin):
#     
#     '''
#     File cache support.
#     '''
#     
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self._cache = Cache(self.database, self.cache_directory)
#     
#     @property
#     def cache(self):
#         return self._cache 
    
def AlgorithmContext(version, configurations={}):
    '''
    Application context mixin, bundles mixins for a Deep Genome based algorithm
        
    Parameters
    ----------
    version
        See chicken_turtle_util.cli.BasicsMixin
    configurations : {configuration_name :: str => help_message :: str}
        Additional configuration files. 'core' as configuration name is reserved
        by DG core.
        
    See also
    --------
    chicken_turtle_util.cli.Context: CLI application context
    '''
    if 'core' in configurations:
        raise ValueError('Configuration name "core" is reserved to Deep Genome core')
    
    configurations = configurations.copy()
    configurations['core'] = _loader.cli_help_message('Configure advanced options such as how exceptional cases should be handled.')
    _ConfigurationsMixin = app.ConfigurationsMixin(configurations)
    
    class _AlgorithmContext(_ConfigurationsMixin, _DatabaseMixin, _CacheDirectoryMixin, _DataDirectoryMixin, app.BasicsMixin(version), app.Context):
        
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self._tasks = Tasks()  # support for deep_genome.core.pipeline
            
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

    return _AlgorithmContext

