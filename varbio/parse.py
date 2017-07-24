# Copyright (C) 2015, 2016 VIB/BEG/UGent - Tim Diels <timdiels.m@gmail.com>
#
# This file is part of varbio.
#
# varbio is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# varbio is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with varbio.  If not, see <http://www.gnu.org/licenses/>.

'''
Parsing of file contents

No cleaning is applied. If your files are dirty, use `varbio.clean` first.
'''

import pandas as pd
import csv
from collections import defaultdict

def expression_matrix(reader):
    '''
    Parse expression matrix in TSV format

    For the exact format, see the File Formats page on RTD.

    Parameters
    ----------
    reader : file object
        Text reader whose content is an expression matrix

    Returns
    -------
    pd.DataFrame({condition_name => [gene_expression :: float]}, index=pd.Index([str], name='gene'))

    Examples
    --------
    >>> with Path('your_file').open() as f:
    ...    matrix = parse.expression_matrix(f)
    '''
    mat = pd.read_table(reader, index_col=0, header=0, engine='python').astype(float)
    mat = mat[mat.index.to_series().notnull()]  # drop rows with no gene name
    mat.index.name = 'gene'
    return mat

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
    reader : file object
        Text reader whose content is a clustering
    name_index : int or None
        Index of the 'column' with the cluster name. If None, each line is an unnamed cluster.

    Returns
    -------
    {cluster_id :: str => items :: {str}}
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
