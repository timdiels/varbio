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
Interpret low-level representations (as produced by the `parse` module) into
higher-level representations
'''

import numpy as np
import logging
from chicken_turtle_util import data_frame as df_

_logger = logging.getLogger(__name__)

def expression_matrix(session, expression_matrix):
    '''
    Interpret and validate expression matrix
    
    Parameters
    ----------
    session : deep_genome.core.database.Session
    expression_matrix : pd.DataFrame({condition_name => [gene_expression :: float]}, index=[gene_symbol :: str])
        
    Returns
    -------
    pd.DataFrame({condition_name => [gene_expression :: float]}, index=[Gene])
        Interpreted matrix
        
    Raises
    ------
    ValueError
        When either:
        
        - a gene appears multiple times with different expression values.
        - expression matrix has a column with a dtype other than float
        
    Examples
    --------
    Clean, parse and interpret expression matrix::
    
        from deep_genome.core import clean, parse, interpret
        with open('my_matrix') as f:
            expression_matrix = interpret.expression_matrix(parse.expression_matrix(clean.plain_text(f)))
    '''
    # Validate expression_matrix
    if not (expression_matrix.dtypes == float).all(): #TODO test
        raise ValueError("Expression matrix values must be of type {}, column types of given matrix:\n{}".format(np.dtype(float), expression_matrix.dtypes.to_string()))
    if expression_matrix.empty:
        raise ValueError('Expression matrix must not be empty')
    
    # Get `Gene`s
    expression_matrix = expression_matrix.copy()
    expression_matrix['_Session__index'] = session.get_genes_by_name(expression_matrix.index.to_series()).apply(list)
    expression_matrix = df_.split_array_like(expression_matrix, '_Session__index')
    
    # Ignore duplicate rows. No warn, they're harmless (though odd)
    expression_matrix.drop_duplicates(inplace=True)
    
    # Validate: no 2 different expression rows should be associated to the same gene
    duplicated = expression_matrix['_Session__index'].duplicated()
    if duplicated.any():
        duplicates = (x.canonical_name.name for x in expression_matrix['_Session__index'][duplicated])
        raise ValueError('Expression matrix has multiple gene expression rows for genes: ' + ', '.join(duplicates))
    
    # Set index to genes
    expression_matrix = expression_matrix.set_index('_Session__index')
    expression_matrix.index.name = None
    
    return expression_matrix
