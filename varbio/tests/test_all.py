# Copyright (C) 2016 VIB/BEG/UGent - Tim Diels <timdiels.m@gmail.com>
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
import warnings

from pytil.data_frame import assert_df_equals
from pytil.pkg_resources import resource_path, resource_stream
import numpy as np
import pandas as pd
import pytest
import scipy.stats

from varbio import (
    pearson, pearson_df, parse_yaml, open_text, ExpressionMatrix, UserError
)


pearsonr = lambda x, y: scipy.stats.pearsonr(x, y)[0]
np.random.seed(0)


class TestParseYAML:

    # Background: https://gitlab.psb.ugent.be/deep_genome/coexpnetviz/issues/7

    @pytest.mark.parametrize('name', (
        'dos', 'incorrect_line_endings', 'latin1', 'utf16_le_bom_dos',
        'utf16le_bom', 'utf8_bom', 'utf8', 'whitespace_tabs', 'whitespace',
    ))
    def test_is_robust(self, name):
        '''
        open_text parse_yaml combo should autodetect encoding, handle
        whitespace and strange line endings
        '''
        path = resource_path(__name__, 'data/parse_yaml_is_robust/{}.yaml'.format(name))
        with open_text(path) as f:
            assert parse_yaml(f) == [
                ['gene', 'col1', 'col2'],
                ['gene1', 12.2, 34.5],
            ]

    def test_include(self):
        actual = parse_yaml(resource_stream(__name__, 'data/parse_yaml_include/main.yaml'))
        assert actual == {'baits': ['one', 'two']}

class TestExpressionMatrixFromDict:

    # See coexpnetviz #7

    def test_happy_days(self):
        matrix = ExpressionMatrix.from_dict({
            'name': 'myname',
            'data': [
                ['mygene', 'col1', 'col2'],
                ['row1', 1.2, 3.4],
                ['row2', 5.6, 7.8],
            ]
        })
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

    # Only trickier validation cases are tested, the rest is only tested
    # manually because in the end you have to check the formatting anyway
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
class TestOther:

    '''
    Test pearson and mutual_information against their automatically vectorised
    equivalent
    '''

    def assert_(self, data, indices):
        with warnings.catch_warnings():
            # Suppress division by zero warnings. For performance, vectorised
            # correlation functions needn't check for rows such as
            # (1, 1, 1), which cause division by zero in pearson.
            warnings.filterwarnings('ignore', 'invalid value encountered in double_scalars', RuntimeWarning)

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

class TestPearsonDf(object):

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
