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

from deep_genome.core import clean, parse
from chicken_turtle_util import data_frame as df_
import logging
import pandas as pd

_logger = logging.getLogger(__name__)

# Note: import functions create their own session as the amount of imported data
# often is too large. Intermediate commits are necessary to avoid sqlalchemy
# crashing or running out of memory. When a session is passed in, we usually
# promise not to commit it; so we don't allow passing in a session.
    
def gene_mapping(context, path):
    '''
    Import gene mapping into database
    
    The file is read and cleaned (without modifying the original) using an
    equivalent of `core.clean.plain_text`, then parsed according to
    `core.parsers.Parser.parse_clustering`. The result is added to
    database.
    
    Parameters
    ----------
    context : deep_genome.core.Context
    path : pathlib.Path
        Path to gene mapping file.
    '''
    with context.database.scoped_session() as session:
        # Read file
        _logger.info('Adding gene mapping from: {}'.format(path))
        with path.open() as f:
            mapping = parse.clustering(clean.plain_text(f))
        mapping = pd.DataFrame(list(mapping.items()), columns=('source', 'destination'))
        mapping['destination'] = mapping['destination'].apply(list)
        mapping = df_.split_array_like(mapping, 'destination')
        
        # Add
        session.add_gene_mapping(mapping)
