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

from deep_blue_genome.core.exception_handlers import UnknownGeneHandler
from deep_blue_genome.core.cache import Cache
from deep_blue_genome.core.database import Database
import plumbum as pb
import tempfile        
        
DatabaseMixin = DatabaseMixin(Database)

class CacheMixin(DatabaseMixin):
    
    '''
    File cache support.
    
    Also throws DatabaseMixin in the mix.
    '''
    
    _cli_options = [
        cli.option(
            '--cache-dir',
            type=click.Path(file_okay=False, writable=True, exists=True, resolve_path=True),
            help='Directory to place cached data. Cached data is not essential, but may speed up subsequent runs.'
        )
    ]
    
    def __init__(self, cache_dir, **kwargs):
        super().__init__(**kwargs)
        self._cache = Cache(self.database, pb.local.path(cache_dir))
    
    @property
    def cache(self):
        return self._cache

    