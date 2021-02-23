# Copyright (C) 2016 VIB/BEG/UGent - Tim Diels <tim@diels.me>
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


from copy import copy
from importlib import resources
import warnings

from pytil.data_frame import assert_df_equals
import numpy as np
import pandas as pd
import pytest
import scipy.stats

from varbio import (
    pearson, pearson_df, parse_yaml, ExpressionMatrix, UserError, parse_csv,
    parse_baits
)


pearsonr = lambda x, y: scipy.stats.pearsonr(x, y)[0]
np.random.seed(0)


class TestParseCSV:

    def _parse(self, name):
        ctx = resources.path('tests.data.parse_csv_is_robust', name)
        with ctx as path:
            return list(parse_csv(path))

    @pytest.mark.parametrize('name', (
        # Autodetect encoding
        'latin1.csv',
        'utf16_le_bom_dos.csv',
        'utf16le_bom.csv',
        'utf8.csv',
        'utf8_bom.csv',

        # Autodetect line ending
        'dos.csv',
        'strange_line_endings.csv',
        'inconsistent_line_endings.csv',

        # Autodetect quotes and separators.
        #
        # We give up if they mix quotes/separators as that is too hard to
        # detect without making too many assumptions (we could show a warning
        # if multiple separators/quote chars appear in the text, but that would
        # cause false positives too; best wait for use cases and add warnings
        # for those specific input examples)
        'no_quotes.csv',
        'semicolon_separator.csv',
        'tab_separator.csv',
        'some_dquote.csv',
        'some_squote.csv',

        # Ignore empty lines
        'empty_lines.csv',
    ))
    def test_is_robust(self, name):
        'See #3'
        assert self._parse(name) == [
            ['gene', 'col1', 'col2'],
            ['gene1', '12.2', '34.5'],
        ]

    def test_trim(self):
        'Trim row/header values, but preserve inner whitespace'
        assert self._parse('untrimmed_value.csv') == [
            ['gene', 'col 	1', 'col2'],
            ['gene 1', '12.2', '34.5'],
        ]

    @pytest.mark.parametrize('name,line_number,col,line', (
        ('empty_header_value.csv', 1, 1, ' 	,col1,col2'),
        ('empty_row_value.csv', 3, 2, 'gene1, 	,34.5'),
    ))
    def test_raise_if_empty_value(self, name, line_number, col, line):
        'Raise if a header/row value is just whitespace'
        with pytest.raises(UserError) as ex:
            self._parse(name)
        msg = str(ex.value).lower()
        assert 'empty' in msg
        assert f'line {line_number}, column {col} (1-based)' in msg
        assert line in msg

    def test_raise_if_cannot_autodetect(self):
        with pytest.raises(UserError) as ex:
            self._parse('inconsistent_col_count_2nd_row.csv')
        msg = str(ex.value).lower()
        assert 'autodetect' in msg
        assert 'first 2 non-empty lines' in msg

    def test_raise_if_inconsistent_col_count(self):
        with pytest.raises(UserError) as ex:
            self._parse('inconsistent_col_count_3rd_row.csv')
        msg = str(ex.value).lower()
        assert 'has 2 columns' in msg
        assert 'expected 3' in msg
        assert 'line 4 (1-based)' in msg
        assert 'gene3,5.6' in msg

    def test_raise_if_empty_file(self):
        with pytest.raises(UserError) as ex:
            self._parse('empty_file.csv')
        msg = str(ex.value).lower()
        assert 'must contain at least a header' in msg

@pytest.mark.parametrize('name', (
    # Handle inconsistent line endings
    'dos.yaml', 'incorrect_line_endings.yaml',

    # Autodetect encoding
    'utf8.yaml', 'latin1.yaml', 'utf8_bom.yaml', 'utf16_le_bom_dos.yaml',
    'utf16le_bom.yaml',

    # Ignore valid whitespace
    'valid_whitespace.yaml',
))
def test_parse_yaml_is_robust(name):
    'See #3'
    with resources.path('tests.data.parse_yaml_is_robust', name) as path:
        assert parse_yaml(path) == [
            ['gene', 'col1', 'col2'],
            ['gene1', 12.2, 34.5],
        ]

def test_parse_yaml_wraps_yaml_error():
    'Raise UserError for yaml errors, such as invalid whitespace'
    with pytest.raises(UserError) as ex:
        ctx = resources.path(
            'tests.data.parse_yaml_is_robust', 'whitespace_tabs.yaml'
        )
        with ctx as path:
            parse_yaml(path)
    msg = str(ex.value)
    assert 'YAML file' in msg
    assert 'whitespace_tabs.yaml' in msg
    assert r'\t' in msg

class TestParseBaits:

    '''
    Encodings and line endings have already been covered by parse_yaml/csv
    tests which use open_text as well
    '''

    def test_worst_valid_input(self):
        with resources.path('tests.data', 'parse_baits') as path:
            actual = parse_baits(path, min_baits=8)
        assert actual == ['1', '2', '3', '4', '5', '6', '7', 'bait8']

    def test_too_few_baits(self):
        with resources.path('tests.data', 'parse_baits') as path:
            with pytest.raises(UserError) as ex:
                parse_baits(path, min_baits=9)
        assert 'at least 9' in str(ex.value)

class TestExpressionMatrixHappyDays:

    @staticmethod
    def _assert(matrix):
        assert matrix.name == 'myname'
        assert_df_equals(matrix.data, pd.DataFrame(
            index=pd.Index(
                # Index name is top left data field
                name='mygene',
                # Index values are first column
                data=['row1', 'row2'],
                # Always treated as str
                dtype=object,
            ),
            columns=pd.Index(
                # Column index values are first row
                ['col1', 'col2'],
                # Always treated as str
                dtype=object,
            ),
            # df values are the remainder of the data matrix
            data=[[1.2, 3.4], [5.6, 7.8]],
        ))

    def test_from_dict(self):
        matrix = ExpressionMatrix.from_dict({
            'name': 'myname',
            'data': [
                ['mygene', 'col1', 'col2'],
                ['row1', 1.2, 3.4],
                ['row2', 5.6, 7.8],
            ]
        })
        self._assert(matrix)

    def test_from_csv(self):
        '''
        relies on _from_array and already got validated a lot by parse_csv so
        we only need test its happy days case
        '''
        matrix = ExpressionMatrix.from_csv(
            name='myname',
            data=[
                ['mygene', 'col1', 'col2'],
                ['row1', '1.2', '3.4'],
                ['row2', '5.6', '7.8'],
            ]
        )
        self._assert(matrix)

class TestExpressionMatrixFromDict:

    def test_convert_values(self, caplog):
        'Ensure rows and cols are str, values are float, with warning'
        matrix = ExpressionMatrix.from_dict({
            'name': 'myname',
            'data': [
                [0, 1, 2],
                [1, 1, '1.3'],
            ]
        })
        assert_df_equals(matrix.data, pd.DataFrame(
            # Row names are converted to str
            index=pd.Index(name='0', data=['1']),
            # Column names are converted to str
            columns=pd.Index(['1', '2']),
            # Values are converted to float
            data=[[1.0, 1.3]],
        ))
        assert (matrix.data.dtypes == float).all()

        # A warning is given when all row or column names are of a number type.
        # Similarly a warning is given for values which aren't of a number
        # type.
        assert 'All row names are numbers' in caplog.text
        assert 'All column names are numbers' in caplog.text
        assert '1 values are not of a number type' in caplog.text
        assert "'1.3'" in caplog.text

    def test_raise_on_single_row(self):
        'Raise user friendly error when only a single row'
        with pytest.raises(UserError) as ex:
            ExpressionMatrix.from_dict({
                'name': 'myname',
                'data': [
                    # This case guards against forgetting [] around the rows
                    'gene', 'col1', 'col2',
                    'row1', 1.2, 3.4,
                ]
            })
        msg = str(ex.value)
        assert '1st' in msg
        assert 'not a list' in msg

    def test_raise_on_inconsistent_column_count(self):
        'Raise user friendly error when column count differs across rows'
        with pytest.raises(UserError) as ex:
            ExpressionMatrix.from_dict({
                'name': 'myname',
                'data': [
                    ['gene', 'col1', 'col2'],
                    ['row1', 1.2],
                ]
            })
        msg = str(ex.value)
        assert '2nd' in msg
        assert 'less columns' in msg

class TestExpressionMatrixFromArray:

    def test_keep_index_and_cols_as_str(self):
        matrix = ExpressionMatrix._from_array(
            name='myname',
            data=np.array([
                ['1', '2'],
                ['3', 1.2],
            ])
        )
        df = matrix.data
        assert df.index.name == '1'
        assert df.index[0] == '3'
        assert df.columns[0] == '2'

    def test_raise_if_duplicate_index(self):
        with pytest.raises(UserError) as ex:
            ExpressionMatrix._from_array(
                name='myname',
                data=np.array([
                    ['gene', 'col1'],
                    ['row1', 1.2],
                    ['row1', 1.3],
                ])
            )
        msg = str(ex.value)
        assert 'duplicate row names' in msg
        assert 'myname' in msg
        assert 'row1' in msg

    def test_raise_if_duplicate_columns(self):
        with pytest.raises(UserError) as ex:
            ExpressionMatrix._from_array(
                name='myname',
                data=np.array([
                    ['gene', 'col1', 'col1'],
                    ['row1', 1.2, 1.3],
                ])
            )
        msg = str(ex.value)
        assert 'duplicate column names' in msg
        assert 'myname' in msg
        assert 'col1' in msg

    @pytest.mark.parametrize('value', ['1.000,2', '1,2'])
    def test_raise_if_invalid_float(self, value):
        with pytest.raises(UserError) as ex:
            ExpressionMatrix._from_array(
                name='myname',
                data=np.array([
                    ['gene', 'col1'],
                    ['row1', value],
                ])
            )
        msg = str(ex.value)
        assert 'could not convert string to float' in msg
        assert value in msg

def unvectorised_pearson(data, indices):
    if not data.size or not len(indices):  # pylint: disable=len-as-condition
        return np.empty((data.shape[0], len(indices)))
    return np.array([np.apply_along_axis(pearsonr, 1, data, data[item]) for item in indices]).T

@pytest.mark.parametrize('data', (
    # Data for which no pair of rows has the same correlation, taking into
    # account Pearson's reflexivity an symmetry.
    np.array([
        [1.1, 2, 3],
        [4.9, 8.1, 7],
        [20, -1, -20.2]
    ]),

    # Data which leads to some NaN and pearson extreme values (1, -1)
    np.array([
        [1, 2, 3],
        [3, 2, 1],
        [2, 4, 6],
        [1, 1, 1],
        [1, 2, np.nan]
    ]),

    # All NaN output
    np.array([
        [1.1, np.nan, 3],
        [4.9, 8.1, np.nan],
        [20, np.nan, -20.2]
    ]),

    # Blob of random data
    np.random.rand(100, 100),
))
class TestPearson:

    'Test pearson against its automatically vectorised equivalent'

    def assert_(self, data, indices):
        with warnings.catch_warnings():
            # Suppress division by zero warnings. For performance, vectorised
            # correlation functions needn't check for rows such as
            # (1, 1, 1), which cause division by zero in pearson.
            warnings.filterwarnings(
                'ignore',
                'invalid value encountered in double_scalars',
                RuntimeWarning
            )

            # Calculate actual
            data_original = data.copy()
            indices_original = copy(indices)
            actual = pearson(data, indices)
            np.testing.assert_array_equal(data, data_original)
            assert indices == indices_original

            # Calculate expected
            #
            # Note: we should actually take twice the default error in allclose as
            # we compare to another algorithm which also has numerical errors
            expected = unvectorised_pearson(data, indices)

            # Assert actual == expected
            np.testing.assert_allclose(actual, expected, equal_nan=True)

    def test_subset_everything(self, data):
        'When subset is everything'
        self.assert_(data, list(range(len(data))))

    def test_subset_range(self, data):
        'When subset is given as a range'
        self.assert_(data, range(3))

    def test_subset_between(self, data):
        'When subset is more than 1 but less than all rows'
        self.assert_(data, list(range(len(data)-1)))

    def test_subset_1_row(self, data):
        'When subset is 1 row'
        self.assert_(data, [1])

    def test_subset_empty(self, data):
        'When subset is empty'
        self.assert_(data, [])

    def test_subset_duplicate(self, data):
        'When subset refers to the same row twice'
        indices = [1, 2, 1]
        self.assert_(data, indices)

    def test_data_1_row(self, data):
        'When data is 1 row'
        self.assert_(data[[0]], [0])

    def test_data_empty(self, data):
        'When data is empty'
        actual = pearson(np.empty((0, 0)), [])
        assert not actual.size
        assert actual.shape == (0, 0)

    def test_data_empty_1d(self, data):
        'When data is empty and its shape is 1D, still return np.empty((0,0))'
        actual = pearson(np.empty((0,)), [])
        assert not actual.size
        assert actual.shape == (0, 0)

class TestPearsonDf:

    @pytest.fixture(scope='session')
    def data(self):
        return pd.DataFrame(
            np.array(range(9)).reshape(3, 3),
            index=map(str, range(3)), dtype=float
        )

    @pytest.fixture(autouse=True)
    def mock_pearson(self, mocker):
        # We only need to test the df wrapper part, not the vectorised pearson
        # calculation itself, so replace it with something simple
        def vectorised(data, indices):
            if not data.size or not len(indices):
                return np.empty((0, 0))
            return np.dot(data, data[indices].T + 1)
        mocker.patch('varbio.pearson', vectorised)

    # TODO rename to _assert
    def assert_(self, data, indices):
        data_original = data.copy()
        subset = data.iloc[indices]
        subset_original = subset.copy()
        expected = pd.DataFrame(
            np.dot(data.values, data.iloc[indices].values.T + 1),
            index=data.index,
            columns=data.index[indices]
        )
        actual = pearson_df(data, subset)
        assert_df_equals(data, data_original)
        assert_df_equals(subset, subset_original)

        assert_df_equals(actual, expected, ignore_order={0, 1})

    def test_duplicate_index(self):
        'When non-unique index, raise ValueError'
        with pytest.raises(ValueError) as ex:
            data = pd.DataFrame(np.zeros((3, 3)), index=[0, 0, 1])
            pearson_df(data, data.loc[1])
        assert 'data.index must be unique' in str(ex.value)

    def test_subset_everything(self, data):
        'When subset is everything'
        self.assert_(data, list(range(len(data))))

    def test_subset_between(self, data):
        'When subset is more than 1 but less than all rows'
        self.assert_(data, list(range(len(data)-1)))

    def test_subset_1_row(self, data):
        'When subset is 1 row'
        self.assert_(data, [1])

    def test_subset_empty(self, data):
        'When subset is empty'
        self.assert_(data, [])

    def test_subset_duplicate(self, data):
        'When subset refers to the same row twice'
        indices = [1, 2, 1]
        self.assert_(data, indices)

    def test_data_1_row(self, data):
        'When data is 1 row'
        self.assert_(data.iloc[[0]], [0])

    def test_data_empty(self, data):
        'When data is empty'
        actual = pearson_df(pd.DataFrame(), pd.DataFrame())
        assert_df_equals(actual, pd.DataFrame())

def test_pearson_df():
    data = pd.DataFrame([[1, 2, 3], [3, 2, 1], [1, 2, 1]], index=['a', 'b', 'c'], dtype=float)
    indices = ['b', 'a']
    actual = pearson_df(data, data.loc[indices])
    expected = pd.DataFrame(
        [[-1, 1], [1, -1], [0, 0]],
        index=['a', 'b', 'c'],
        columns=indices
    )
    assert actual.index.equals(expected.index) # TODO allow reorder, use df_equals(ignore_order={0,1}, values_close=True) values_close: if True, use np.isclose(df.values); later can add a dict as its value, being the kwargs to isclose
    assert actual.columns.equals(expected.columns)
    np.testing.assert_allclose(actual.values, expected.values, equal_nan=True)
