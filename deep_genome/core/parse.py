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

#TODO no such thing as strict_input_validation
'''
Parsing of expression matrices, clusterings, ...

When a parsing error occurs, a warning is logged. With strict_input_validation
on, an exception is raised, else a best-effort approach is used to recover,
within sane bounds.

No cleaning is applied. If your files are dirty, use
`deep_genome.core.clean` first.

The results of parsing may still be fairly raw, further interpretation and
validation happens when the data is added to Database. E.g. parsing will not
remove genes that are duplicated (not in the biological sense) in the input.
'''

import pandas as pd
import csv
from chicken_turtle_util.data_frame import split_array_like
from collections import defaultdict

def expression_matrix(reader):
    '''
    Parse expression matrix in tabular plain text format
    
    The expected format is a table with 1 header line. The first column is the
    gene (or gene variant) name. Each other column contains the gene expression
    values under a certain condition. Rows are separated by
    '\n', columns are separated by whitespace.
    
    Parameters
    ----------
    reader : io.BufferedReader
        Text reader whose content is an expression matrix
    
    Returns
    -------
    pd.DataFrame({condition_name => [gene_expression :: float]}, index=pd.Index([str], name='gene'))
    
    Examples
    --------
    Format example::
    
        ignored<tab>condition1<tab>condition2
        gene1<tab>1.5<tab>5
        gene2<tab>.89<tab>-.1
        
    Usage:
    
    >>> with Path('your_file').open() as f:
    >>>    matrix = parse.expression_matrix(f)
    '''
    mat = pd.read_table(reader, index_col=0, header=0, engine='python').astype(float)
    mat = mat[mat.index.to_series().notnull()]  # TODO log warnings for dropped rows #TODO do we ever need this?
    mat.index.name = 'gene'
    return mat

#TODO strictness should have us fail on duplicate genes
# Note for old calls: set_.merge_by_overlap + #TODO in DG make a NamedSet(set) hidden in some module that has names property and merges its names with other `NamedSet`s when merging the set. Preferably find a way to call it metadata instead of names MetaSet or something)
def clustering(reader, name_index=0):
    '''
    Parse plain text formatted clustering
    
    Each cluster can be specified on a single line::
    
        cluster1<tab>item1<tab>item2<tab>item3
        cluster2<tab>item5
        
    or across multiple lines::
    
        cluster1<tab>item1
        cluster1<tab>item2
        cluster2<tab>item5
        cluster1<tab>item3
        
    or a combination of the two::
    
        cluster1<tab>item1<tab>item2
        cluster2<tab>item5
        cluster1<tab>item3
    
    Items may be assigned to multiple clusters. Clusters are treated as sets.
    When an item appears in a cluster multiple times, a warning is logged and
    the duplicate is ignored.
    
    Parameters
    ----------
    reader : io.BufferedReader
        Text reader whose content is a clustering
    name_index : int
        Index of the 'column' with the cluster name. If None, each line is an unnamed cluster.
        
    Returns
    -------
    {cluster_id :: str => items :: {str}})
        Clustering as multi-dict.
    '''
    if name_index and name_index < 0:
        raise ValueError('name_index must be >=0 or None')
    reader = csv.reader(reader, delimiter='\t')
    clustering = defaultdict(set)
    if name_index is not None:
        for row in reader:
            clustering[row[name_index]] |= set(row[0:name_index] + row[name_index+1:])
    else:
        for i, row in enumerate(reader):
            clustering[i] = set(row)
    clustering = dict(clustering)
    return clustering
