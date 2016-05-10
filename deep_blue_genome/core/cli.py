# Copyright (C) 2015, 2016 VIB/BEG/UGent - Tim Diels <timdiels.m@gmail.com>
# 
# This file is part of Deep Blue Genome.
# 
# Deep Blue Genome is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# Deep Blue Genome is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public License
# along with Deep Blue Genome.  If not, see <http://www.gnu.org/licenses/>.

'''
Mixins to build a Context class (or 'Application' class if you prefer)

To create a context class: e.g. class MyContext(Mixin1, Mixin2, ...): pass
'''

from chicken_turtle_util import cli
from chicken_turtle_util.configuration import ConfigurationLoader
from deep_blue_genome.core.configuration import Configuration
from deep_blue_genome.core.cache import Cache
from deep_blue_genome.core.database import Database
from pathlib import Path
from textwrap import dedent
import plumbum as pb
import click
import xdg

DatabaseMixin = cli.DatabaseMixin(Database)

# ConfigurationMixin
_loader = ConfigurationLoader('deep_blue_genome.core', 'deep_blue_genome', 'core')    

ConfigurationMixin = cli.ConfigurationMixin(
    lambda context, path: Configuration(_loader.load(path)), 
    _loader.cli_help_message('Using core.conf, you can configure more advanced options such as how exceptional cases should be handled.')
)

ConfigurationMixin.__doc__ = dedent('''\
    Like `chicken_turtle_util.cli.ConfigurationMixin`, but `context.configuration` is of type `deep_blue_genome.core.Configuration`
    
    Configuration is loaded from `core.conf` files using ConfigurationLoader.
    ''')    

#
def AlgorithmMixin(version):
    '''
    Application context mixin, bundles mixins for a Deep Blue Genome based algorithm
        
    Parameters
    ----------
    version
        See chicken_turtle_util.cli.BasicsMixin
        
    See also
    --------
    chicken_turtle_util.cli.Context: CLI application context
    '''
    class _AlgorithmMixin(ConfigurationMixin, DatabaseMixin, cli.DataDirectoryMixin('deep_blue_genome'), cli.BasicsMixin(version), cli.Context):
        pass
    return _AlgorithmMixin

class CacheMixin(DatabaseMixin):
    
    '''
    File cache support.
    
    Also throws DatabaseMixin in the mix.
    '''
    
    _cli_options = [ #TODO is old styled, silently fails
        cli.option(
            '--cache-dir',
            type=click.Path(file_okay=False, writable=True, exists=True, resolve_path=True),
            help='Directory to place cached data. Cached data is not essential, but may speed up subsequent runs considerably.'
        )
    ]
    
    def __init__(self, cache_dir, **kwargs):
        super().__init__(**kwargs)
        self._cache = Cache(self.database, pb.local.path(cache_dir))
    
    @property
    def cache(self):
        return self._cache 
        