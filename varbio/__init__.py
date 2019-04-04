# Copyright (C) 2015 VIB/BEG/UGent - Tim Diels <timdiels.m@gmail.com>
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


__version__ = '3.0.0.dev'


from contextlib import contextmanager
from numbers import Number
from pathlib import Path
import logging

import attr
import chardet
import humanize
import numpy as np
import pandas as pd
import yaml

from varbio.yaml_loader import YAMLIncludeLoader


_logger = logging.getLogger(__name__)


class UserError(Exception):
    '''
    Error caused by user error

    Has user friendly message. The intent is for a CLI to show the message to
    the user without a stack trace (as those are scary). At the same time the
    API can still raise these errors directly to the API user; otherwise we'd
    have just printed the error right away as this is not a type of error the
    program itself can recover from; user input is required to fix it.
    '''

@attr.s(slots=True)
class ExpressionMatrix:

    # Reasoning for the validation: https://gitlab.psb.ugent.be/deep_genome/coexpnetviz/issues/7

    name = attr.ib()
    'str'

    data = attr.ib()
    'pandas.DataFrame with float values'

    _matrix_example_msg = (
        # Always show the YAML example, even API users' dicts may originate
        # from YAML
        'Example of a valid matrix in YAML:\n\n'
        '    name: matrix1\n'
        '    data: [\n'
        '        [genes, col1, col2],\n'
        '        [gene1, 12.34, 132],\n'
        '    ]\n\n'
        'Or as a python/yaml dict: {\n'
        '    "name": "matrix1",\n'
        '    "data": [\n'
        '        ["genes", "col1", "col2"],\n'
        '        ["gene1", 12.34, 132],\n'
        '    ]\n'
        '}'
    )

    # Ensure there are no row/col duplicates
    #set(rows) TODO probably easiest with pd.indices

    @classmethod
    def from_dict(cls, matrix):
        # Usually from_dict is called as part of parse_yaml so error messages
        # somewhat assume the input is actually yaml
        def _raise(msg):
            raise UserError(msg + '\n\n' + cls._matrix_example_msg)

        # Basic validation
        if not isinstance(matrix, dict):
            _raise(
                'The matrix should be a dictionary.\n\nGiven matrix: {!r}'
                .format(matrix)
            )
        if 'name' not in matrix:
            _raise('Please provide a name for the matrix.')
        if 'data' not in matrix:
            _raise('Please provide the data of the matrix.')
        data = matrix['data']
        if not isinstance(data, list):
            _raise('data should be a list.\n\nGiven data: {!r}'.format(data))
        if not data:
            _raise('data must not be empty.\n\nGiven data: {!r}'.format(data))

        # Rows should be lists
        for i, row in enumerate(data, 1):
            if not isinstance(row, list):
                _raise(
                    'The {} row is not a list. Perhaps you forgot to wrap '
                    'the row in []?\n\nGiven row: {!r}'
                    .format(humanize.ordinal(i), row)
                )

        # Column count should be consistent across rows
        column_count = len(data[0])
        for i, row in enumerate(data[1:], 2):
            if len(row) != column_count:
                difference = 'more' if len(row) > column_count else 'less'
                _raise(
                    'The {} row has {} columns than previous rows. All rows '
                    'should have the same length.\n\nGiven row: {!r}'
                    .format(humanize.ordinal(i), difference, row)
                )

        # dtype=object to preserve the type, else the whole lot can become str
        # for example
        data = np.array(data, dtype=object)
        columns = data[0, 1:]
        rows = data[1:, 0]
        values = data[1:, 1:]

        # Warn if all row or column names are numbers. This guards against an input
        # like [[1, 2], [3, 4]]; i.e. user forgot to add a header or row names.
        should_warn = False
        if all(isinstance(row, Number) for row in rows):
            should_warn = True
            _logger.warning(
                'All row names are numbers, perhaps you forgot to add the row '
                'names? If this is intended, consider wrapping them in '
                'quotes (\'") to avoid this warning.'
            )
        if all(isinstance(column, Number) for column in columns):
            should_warn = True
            _logger.warning(
                'All column names are numbers, perhaps you forgot to add the header row? '
                'If this is intended, consider wrapping them in '
                'quotes (\'") to avoid this warning.'
            )
        # We also warn for values as this is also not something you'd tend to
        # normally do
        odd_values = [value for value in values.ravel() if not isinstance(value, Number)]
        if odd_values:
            should_warn = True
            _logger.warning(
                '{} values are not of a number type, it is preferred '
                'to specify these as number literals (e.g. 3, .inf, -.inf, '
                '.nan) instead of e.g. strings. Given non-number values: {}{}'
                .format(
                    len(odd_values),
                    ', '.join(map(repr, odd_values[:10])),
                    ', ...' if len(odd_values) > 10 else '',
                )
            )

        if should_warn:
            _logger.warning('\n' + cls._matrix_example_msg)

        # Convert values to the correct type and create a df from them
        index = pd.Index(rows.astype(str), name=str(data[0, 0]))
        df = pd.DataFrame(values, index=index, columns=columns.astype(str), dtype=float)
        return cls(name=matrix['name'], data=df)

@contextmanager
def open_text(path):
    '''
    Robustly open text file

    Autodetect encoding. Python's universal newlines takes care of
    strange/mixed line endings.

    Parameters
    ----------
    path : ~pathlib.Path

    Returns
    -------
    file
        File object of the opened text file
    '''
    with path.open('rb') as f:
        encoding = chardet.detect(f.read())['encoding']
    with path.open(encoding=encoding) as f:
        yield f

def parse_yaml(file):
    '''
    Robustly parse yaml with support for !include

    Parameters
    ----------
    file : file or pathlib.Path
        File object (mode=r) or path. If a path, it is read using open_text.

    Returns
    -------
    dict or list
        Parsed YAML as returned by `yaml.load`.
    '''
    def load(f):
        return yaml.load(f, YAMLIncludeLoader)
    if isinstance(file, Path):
        with open_text(file) as f:
            return load(f)
    return load(file)

def pearson(data, indices):
    '''
    Get Pearson's r of each row in a 2D array compared to a subset thereof.

    Parameters
    ----------
    data : ArrayLike[float]
        2D array for which to calculate correlations between rows.
    indices
        Indices to derive the subset ``data[indices]`` to compare against. You
        may use any form of numpy indexing.

    Returns
    -------
    correlation_matrix : ArrayLike[float]
        2D array containing all correlations. ``correlation_matrix[i,j]``
        contains ``correlation_function(data[i], data[indices][j]``. Its shape
        is ``(len(data), len(indices))``.

    Notes
    -----
    The current implementation is a vectorised form of ``gsl_stats_correlation``
    from the GNU Scientific Library. Unlike GSL's implementation,
    correlations are clipped to ``[-1, 1]``.

    Pearson's r is also, perhaps more commonly, known as the product-moment
    correlation coefficient.
    '''
    # `not len` is required instead of just `not`, otherwise you get
    # 'ValueError: The truth value of a Int64Index is ambiguous'
    #
    # pylint: disable=len-as-condition
    if not data.size or not len(indices):
        return np.empty((data.shape[0], len(indices)))

    matrix = data
    mean = matrix[:, 0].copy()
    delta = np.empty(matrix.shape[0])
    sum_sq = np.zeros(matrix.shape[0])  # sum of squares
    sum_cross = np.zeros((matrix.shape[0], len(indices)))

    for i in range(1, matrix.shape[1]):
        ratio = i / (i+1)
        delta = matrix[:, i] - mean
        sum_sq += (delta**2) * ratio
        sum_cross += np.outer(delta, delta[indices]) * ratio
        mean += delta / (i+1)

    sum_sq = np.sqrt(sum_sq)
    with np.errstate(divide='ignore', invalid='ignore'):  # divide by zero, it happens
        correlations = sum_cross / np.outer(sum_sq, sum_sq[indices])
    np.clip(correlations, -1, 1, correlations)
    return correlations

def pearson_df(data, subset):
    '''
    Get Pearson correlation of each row in a DataFrame compared to a subset
    thereof.

    Parameters
    ----------
    data : ~pandas.DataFrame[float]
        Data for which to calculate correlations between rows.
    subset
        Subset of ``data`` to compare against. ``subset.index`` must be a subset
        of ``data.index``.

    Returns
    -------
    correlation_matrix : pandas.DataFrame[float]
        Data frame with all correlations. ``correlation_matrix.iloc[i,j]``
        contains the correlation between ``data.iloc[i]`` and
        ``subset.iloc[j]``. The data frame has ``data.index`` as index and
        ``subset.index`` as columns.
    '''
    if not data.index.is_unique:
        raise ValueError('data.index must be unique')
    correlations = pearson(data.values, subset.index.map(data.index.get_loc))
    correlations = pd.DataFrame(correlations, index=data.index, columns=subset.index)
    return correlations
