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

'''
Test deep_genome.core.correlation
'''

from sklearn.metrics import mutual_info_score
from chicken_turtle_util import data_frame as df_
from deep_genome.core import correlation
from functools import partial
from copy import copy
import scipy.stats
import numpy as np
import pandas as pd
import pytest

pearsonr = lambda x, y: scipy.stats.pearsonr(x,y)[0]
   
class TestGeneric(object):
    
    @pytest.fixture
    def correlation_function(self):
        '''
        correlation function mock
        '''
        return lambda x, y: float(hash((tuple(x), tuple(y))))
    
    @pytest.fixture
    def data(self):
        '''
        Data for which no pair of rows has the same correlation (according to
        our correlation_function)
        '''
        return np.array([
            [1.1, 2, 3],
            [4.9, 8.1, 7],
            [20, -1, -20.2] 
        ])
        
    @pytest.fixture
    def vectorised(self, correlation_function):
        return partial(correlation.generic, correlation_function)
    
    @pytest.fixture
    def vectorised_nan(self, correlation_function):
        return partial(correlation.generic, lambda x, y: np.nan)
        
    def assert_content(self, matrix, correlation_function, data, subset):
        for i, row_i in enumerate(data):
            for j, row_j in enumerate(subset):
                assert matrix[i, j] == correlation_function(row_i, row_j)
        
    def test_meta(self, correlation_function, data):
        '''
        Meta test that `correlation_function` applied to `data` yields a matrix
        of all unique values
        '''
        corrs = set()
        for row1 in data:
            for row2 in data:
                corrs.add(correlation_function(row1, row2)) 
        assert len(corrs) == data.size
                
    def test_subset_everything(self, correlation_function, data, vectorised):
        '''
        When subset is everything
        '''
        data_original = data.copy()
        indices = [0, 1, 2]
        indices_original = copy(indices)
        matrix = vectorised(data, indices)
        np.testing.assert_array_equal(data, data_original)
        assert indices == indices_original
        assert matrix.shape == (3, 3)
        self.assert_content(matrix, correlation_function, data, data)
        
    def test_subset_range(self, correlation_function, data, vectorised):
        '''
        When subset is given as a range
        '''
        matrix = vectorised(data, range(3))
        assert matrix.shape == (3, 3)
        self.assert_content(matrix, correlation_function, data, data)
        
    def test_subset_between(self, correlation_function, data, vectorised):
        '''
        When subset is more than 1 but less than all rows
        '''
        matrix = vectorised(data, [0, 2])
        assert matrix.shape == (3, 2)
        self.assert_content(matrix, correlation_function, data, data[[0,2]])
        
    def test_subset_1_row(self, correlation_function, data, vectorised):
        '''
        When subset is 1 row
        '''
        matrix = vectorised(data, [1])
        assert matrix.shape == (3, 1)
        self.assert_content(matrix, correlation_function, data, data[[1]])
        
    def test_subset_empty(self, data, vectorised):
        '''
        When subset is empty
        '''
        matrix = vectorised(data, [])
        assert matrix.size == 0
        
    def test_subset_duplicate(self, correlation_function, data, vectorised):
        '''
        When subset refers to the same row twice
        '''
        indices = [1, 2, 1]
        matrix = vectorised(data, indices)
        assert matrix.shape == (3, 3)
        self.assert_content(matrix, correlation_function, data, data[indices])
        
    def test_data_1_row(self, correlation_function, data, vectorised):
        '''
        When data is 1 row
        '''
        matrix = vectorised(data[[0]], [0])
        assert matrix.shape == (1, 1)
        self.assert_content(matrix, correlation_function, data[[0]], data[[0]])
        
    def test_data_empty(self, vectorised):
        '''
        When data is empty
        '''
        actual = vectorised(np.empty((0,0)), [])
        assert not actual.size
        assert actual.shape == (0,0)
         
    def test_data_empty_bad(self, vectorised):
        '''
        When data is empty and its shape is 1D, still return np.empty((0,0))
        '''
        actual = vectorised(np.empty((0,)), [])
        assert not actual.size
        assert actual.shape == (0,0)
        
    def test_nan(self, data, vectorised_nan):
        '''
        When correlation function returns NaN
        '''
        matrix = vectorised_nan(data, [0,1])
        assert matrix.shape == (3, 2)
        assert np.isnan(matrix).all()
        
np.random.seed(0)
@pytest.mark.parametrize('correlation_function, data', (
    ###########
    # Pearson
     
    # Data for which no pair of rows has the same correlation, taking into account Pearson's reflexivity an symmetry.
    ((correlation.pearson, pearsonr), np.array([
        [1.1, 2, 3],
        [4.9, 8.1, 7],
        [20, -1, -20.2] 
    ])),
      
    # Data which leads to some NaN and pearson extreme values (1, -1)
    ((correlation.pearson, pearsonr), np.array([
        [1, 2, 3],
        [3, 2, 1],
        [2, 4, 6],
        [1, 1, 1],
        [1, 2, np.nan] 
    ])),
      
    # All NaN output
    ((correlation.pearson, pearsonr), np.array([
        [1.1, np.nan, 3],
        [4.9, 8.1, np.nan],
        [20, np.nan, -20.2] 
    ])),
    
    # Large blob of random data
    ((correlation.pearson, pearsonr), np.random.rand(100,100)),  #TODO scale up to 1000,100 once vectorise is optimised. You'll want a different size for mutual info unless it happens to have the same performance
     
    ######################
    # Mutual information
    
    #TODO add some special cases
    
    # Large blob of random data
    ((correlation.mutual_information, mutual_info_score), np.random.rand(100,100)),  #TODO scale up to 1000,100 once vectorise is optimised. You'll want a different size for mutual info unless it happens to have the same performance
))
class TestOther(object):
     
    '''
    Test pearson and mutual_information against their automatically vectorised
    equivalent
    '''
     
    def assert_(self, correlation_function, data, indices):
        vectorised, plain = correlation_function
        data_original = data.copy()
        indices_original = copy(indices)
        actual = vectorised(data, indices)
        np.testing.assert_array_equal(data, data_original)
        assert indices == indices_original
        
        # Note: we should actually take twice the default error in allclose as
        # we compare to another algorithm which also has numerical errors
        expected = correlation.generic(lambda x, y: plain(x, y), data, indices)
        np.testing.assert_allclose(actual, expected, equal_nan=True)
         
    def test_subset_everything(self, correlation_function, data):
        '''
        When subset is everything
        '''
        self.assert_(correlation_function, data, list(range(len(data))))
         
    def test_subset_range(self, correlation_function, data):
        '''
        When subset is given as a range
        '''
        self.assert_(correlation_function, data, range(3))
         
    def test_subset_between(self, correlation_function, data):
        '''
        When subset is more than 1 but less than all rows
        '''
        self.assert_(correlation_function, data, list(range(len(data)-1)))
         
    def test_subset_1_row(self, correlation_function, data):
        '''
        When subset is 1 row
        '''
        self.assert_(correlation_function, data, [1])
         
    def test_subset_empty(self, correlation_function, data):
        '''
        When subset is empty
        '''
        self.assert_(correlation_function, data, [])
         
    def test_subset_duplicate(self, correlation_function, data):
        '''
        When subset refers to the same row twice 
        '''
        indices = [1, 2, 1]
        self.assert_(correlation_function, data, indices)
         
    def test_data_1_row(self, correlation_function, data):
        '''
        When data is 1 row
        '''
        self.assert_(correlation_function, data[[0]], [0])
        
    def test_data_empty(self, correlation_function, data):
        '''
        When data is empty
        '''
        actual = correlation_function[0](np.empty((0,0)), [])
        assert not actual.size
        assert actual.shape == (0,0)
         
    def test_data_empty_1d(self, correlation_function, data):
        '''
        When data is empty and its shape is 1D, still return np.empty((0,0))
        '''
        actual = correlation_function[0](np.empty((0,)), [])
        assert not actual.size
        assert actual.shape == (0,0)

class TestGenericDF(object):
    
    @pytest.fixture
    def data(self):
        return pd.DataFrame(np.array(range(9)).reshape(3,3), index=map(str, range(3)))
    
    @pytest.fixture
    def vectorised_df(self):
        def vectorised(data, indices):
            '''
            Simple vectorised 'correlation' function
            '''
            if not data.size or not len(indices):
                return np.empty((0,0))
            return np.dot(data, data[indices].T + 1)
        return partial(correlation.generic_df, vectorised)
    
    def assert_(self, correlation_function, data, indices):
        data_original = data.copy()
        subset = data.iloc[indices]
        subset_original = subset.copy()
        expected = pd.DataFrame(np.dot(data.values, data.iloc[indices].values.T + 1), index=data.index, columns=data.index[indices])
        actual = correlation_function(data, subset)
        assert df_.equals(data, data_original)
        assert df_.equals(subset, subset_original)
        
        assert df_.equals(actual, expected, ignore_order={0,1}) 
        
    def test_duplicate_index(self, vectorised_df):
        '''
        When non-unique index, raise ValueError
        '''
        with pytest.raises(ValueError) as ex:
            data = pd.DataFrame(np.zeros((3,3)), index=[0,0,1])
            vectorised_df(data, data.loc[1])
        assert '``data.index`` must be unique' in str(ex.value)
        
    def test_subset_everything(self, vectorised_df, data):
        '''
        When subset is everything
        '''
        self.assert_(vectorised_df, data, list(range(len(data))))
         
    def test_subset_between(self, vectorised_df, data):
        '''
        When subset is more than 1 but less than all rows
        '''
        self.assert_(vectorised_df, data, list(range(len(data)-1)))
         
    def test_subset_1_row(self, vectorised_df, data):
        '''
        When subset is 1 row
        '''
        self.assert_(vectorised_df, data, [1])
         
    def test_subset_empty(self, vectorised_df, data):
        '''
        When subset is empty
        '''
        self.assert_(vectorised_df, data, [])
         
    def test_subset_duplicate(self, vectorised_df, data):
        '''
        When subset refers to the same row twice 
        '''
        indices = [1, 2, 1]
        self.assert_(vectorised_df, data, indices)
         
    def test_data_1_row(self, vectorised_df, data):
        '''
        When data is 1 row
        '''
        self.assert_(vectorised_df, data.iloc[[0]], [0])
        
    def test_data_empty(self, vectorised_df, data):
        '''
        When data is empty
        '''
        actual = vectorised_df(pd.DataFrame(), pd.DataFrame())
        assert df_.equals(actual, pd.DataFrame())
         
def test_pearson_df():
    data = pd.DataFrame([[1,2,3], [3,2,1], [1,2,1]], index=['a', 'b', 'c'], dtype=float)
    indices = ['b', 'a']
    actual = correlation.pearson_df(data, data.loc[indices])
    expected = pd.DataFrame([[-1, 1], [1, -1], [0, 0]], index=['a', 'b', 'c'], columns=['b', 'a'])
    assert actual.index.equals(expected.index) # TODO allow reorder, use df_.equals(ignore_order={0,1}, values_close=True) values_close: if True, use np.isclose(df.values); later can add a dict as its value, being the kwargs to isclose
    assert actual.columns.equals(expected.columns)
    np.testing.assert_allclose(actual.values, expected.values, equal_nan=True)
    
def test_mutual_information_df():
    data = pd.DataFrame([[1,2,3], [3,2,1], [1,2,1]], index=['a', 'b', 'c'], dtype=float)
    indices = ['b', 'a']
    subset = data.loc[indices]
    expected = correlation.generic_df(partial(correlation.generic, mutual_info_score), data, subset)
    actual = correlation.mutual_information_df(data, subset)
    
    assert actual.index.equals(expected.index)  # TODO allow reorder, use df_.equals(ignore_order={0,1}, values_close=True) values_close: if True, use np.isclose(df.values); later can add a dict as its value, being the kwargs to isclose
    assert actual.columns.equals(expected.columns)
    np.testing.assert_allclose(actual.values, expected.values, equal_nan=True)
    
#TODO performance test where you use `generic` as baseline. Just set some min amount of speedup compared to generic. Of course you have to change that when optimising generic itself
# Performance tests should run separately without xdist for better accuracy