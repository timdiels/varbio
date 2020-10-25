# Copyright (C) 2020 VIB/BEG/UGent - Tim Diels <tim@diels.me>
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

from numbers import Number
from textwrap import dedent
import logging
import re

import attr
import humanize
import numpy as np
import pandas as pd
import yaml

from varbio import __version__
from varbio._util import open_text, UserError, join_lines


@attr.s(slots=True, repr=False, frozen=True)
class ExpressionMatrix:

    # Validation: see #3
    '''
    Gene expression matrix.

    Parameters
    ----------
    name : str
        Unique name of the matrix.
    data : ~pandas.DataFrame
        Gene expression data. The data frame is a matrix of gene expression of
        type `float` with genes as index of type `str`.
    '''

    name = attr.ib()
    'str'

    data = attr.ib()
    'pandas.DataFrame with float values'

    _matrix_example_msg = dedent(
        '''
        Example of a valid matrix in YAML:

            name: matrix1
            data: [
                [genes, col1, col2],
                [gene1, 12.34, 132],
            ]

        Or as a python/yaml dict: {
            "name": "matrix1",
            "data": [
                ["genes", "col1", "col2"],
                ["gene1", 12.34, 132],
            ]
        }
        '''
    )

    def __repr__(self):
        return f'ExpressionMatrix({self.name!r})'

    @name.validator
    def _validate_name(self, _, name):
        if '\0' in name:
            raise ValueError('Name must not contain "\0" (nul character)')
        if not name:
            raise ValueError('Name must not be empty')
        if not name.strip():
            raise ValueError('Name must not be whitespace only')
        if name != name.strip():
            raise ValueError('Name must not be surrounded by whitespace')

    @data.validator
    def _validate_data(self, _, data):
        self._raise_if_duplicates(data.index, 'row')
        self._raise_if_duplicates(data.columns, 'column')

    def _raise_if_duplicates(self, index, index_name):
        duplicates = index[index.duplicated()]
        if not duplicates.empty:
            duplicates = ', '.join(map(repr, duplicates))
            raise ValueError(
                f'{self.name} has duplicate {index_name} names: {duplicates}'
            )

    @classmethod
    def from_dict(cls, matrix):
        # Usually from_dict is called as part of parse_yaml so error messages
        # somewhat assume the input is actually yaml
        def _raise(msg):
            raise UserError(f'{msg}\n\n{cls._matrix_example_msg}')

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
        cls._warn_if_unexpected_type(data)

        return cls._from_array(matrix['name'], data)

    @classmethod
    def _warn_if_unexpected_type(cls, data):
        columns = data[0, 1:]
        rows = data[1:, 0]
        values = data[1:, 1:]
        should_warn = False

        # Warn if all row or column names are numbers. This guards against an input
        # like [[1, 2], [3, 4]]; i.e. user forgot to add a header or row names.
        if all(isinstance(row, Number) for row in rows):
            should_warn = True
            logging.warning(
                'All row names are numbers, perhaps you forgot to add the row '
                'names? If this is intended, consider wrapping them in '
                'quotes (\'") to avoid this warning.'
            )
        if all(isinstance(column, Number) for column in columns):
            should_warn = True
            logging.warning(
                'All column names are numbers, perhaps you forgot to add the header row? '
                'If this is intended, consider wrapping them in '
                'quotes (\'") to avoid this warning.'
            )

        # We also warn for values as this is also not something you'd tend to
        # normally do
        odd_values = [value for value in values.ravel() if not isinstance(value, Number)]
        if odd_values:
            should_warn = True
            odd_count = len(odd_values)
            odd_values = list(map(repr, odd_values[:10]))
            if odd_count > 10:
                odd_values.append('...')
            odd_values = ', '.join(odd_values)
            logging.warning(join_lines(
                f'''
                {odd_count} values are not of a number type, it is preferred to
                specify these as number literals (e.g. 3, .inf, -.inf, .nan)
                instead of e.g. strings. Given non-number values: {odd_values}
                '''
            ))

        if should_warn:
            logging.warning(f'\n{cls._matrix_example_msg}')

    @classmethod
    def from_csv(cls, name, data):
        'Construct from data parsed with parse_csv'
        data = np.array(list(data), dtype=object)
        return cls._from_array(name, data)

    @classmethod
    def _from_array(cls, name, data):
        # Convert values to the correct type and create a df from them
        columns = data[0, 1:]
        rows = data[1:, 0]
        values = data[1:, 1:]
        index = pd.Index(rows.astype(str), name=str(data[0, 0]))
        columns = columns.astype(str)
        try:
            df = pd.DataFrame(values, index=index, columns=columns, dtype=float)
        except ValueError as ex:
            if 'convert string to float' in str(ex):
                msg = f'Invalid float value: {ex.__cause__.args[0]}'
                raise UserError(msg) from ex
        try:
            return cls(name=name, data=df)
        except ValueError as ex:
            raise UserError(ex.args[0]) from ex

def parse_yaml(path):
    '''
    Robustly parse yaml

    Parameters
    ----------
    path : ~pathlib.Path

    Returns
    -------
    dict or list
        Parsed YAML as returned by `yaml.load`.
    '''
    with open_text(path) as f:
        # C loaders are faster than regular loaders but require libyaml,
        # performance is not an issue in our case so we rather ditch the
        # dependency. SafeLoader disables insecure features which we don't need,
        # e.g. arbitrary code execution if I recall correctly; more generally it
        # reduces the attack surface from parsing untrusted inputs.
        try:
            return yaml.load(f, yaml.SafeLoader)
        except yaml.YAMLError as ex:
            raise UserError(f'YAML file contains error: {ex}') from ex

def parse_baits(path, min_baits):
    '''
    Robustly parse baits file

    Parameters
    ----------
    path : ~pathlib.Path

    Returns
    -------
    list
        Bait names
    '''
    with open_text(path) as f:
        # Doesn't work for gene names which contain spaces, would have to use yaml
        # input for that
        baits = [
            bait
            for bait in re.split(r'[\s,;]+', f.read())
            if bait
        ]

        if len(baits) < min_baits:
            raise UserError(
                f'{path} needs at least {min_baits} baits, but contains only {len(baits)}'
            )

        return baits

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

def init_logging(program, version, log_file):
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('{asctime} {levelname[0]}: {message}', style='{')

    # Log to stderr
    stderr_handler = logging.StreamHandler() # to stderr
    stderr_handler.setLevel(logging.DEBUG)
    stderr_handler.setFormatter(formatter)
    root_logger.addHandler(stderr_handler)

    # and to file
    file_handler = logging.FileHandler(str(log_file))
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Log versions
    logging.info(f'{program} version: {version}')
    logging.info(f'varbio version: {__version__}')
