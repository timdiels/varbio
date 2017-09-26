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
Parsing of file contents.

No cleaning is applied. If your files are dirty, use `varbio.clean` first.
'''

import pandas as pd
import csv
from collections import defaultdict

def expression_matrix(reader):
    '''
    Parse expression matrix in TSV format.

    For a description of the file format, see the :ref:`Expression matrix file`
    section in the documentation.

    Parameters
    ----------
    reader : ~typing.TextIO
        Text reader whose content is an expression matrix.

    Returns
    -------
    ~pandas.DataFrame[Float]
        Expression matrix as data frame with condition names as columns and gene
        names (or whatever the expression matrix file uses) as index.

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
    Parse plain text formatted clustering.

    For a description of the file format, see the :ref:`Clustering file`
    section in the documentation.

    Warnings are issued for items appearing multiple times in a cluster (as this
    indicates a bug in the software that created the clustering).

    Parameters
    ----------
    reader : ~typing.TextIO
        Text reader whose content is a clustering.
    name_index : int or None
        Index of the 'column' with the cluster name. If `None`, each line is an
        unnamed cluster.

    Returns
    -------
    ~typing.Dict[str, ~typing.Set[str]]
        Clustering as multi-dict mapping each cluster id to its items.
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
