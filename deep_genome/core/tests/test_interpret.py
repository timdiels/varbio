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
Test deep_genome.core.interpret.expression_matrix
'''

from chicken_turtle_util import data_frame as df_
from deep_genome.core import interpret
from textwrap import dedent
import pandas as pd
import numpy as np
import pytest

class TestExpressionMatrix(object):
    
    '''
    Test interpret.expression_matrix.expression_matrix
    '''
    
    _expression_matrix_df = pd.DataFrame({'condition1': [1.1, 3.3], 'condition2': [2.2, 4.4]}, index=['gene1', 'gene2'])
    _expression_matrix_df_duplicate_row = pd.DataFrame({'condition1': [1.1, 3.3, 3.3], 'condition2': [1.1, 4.4, 4.4]}, index=['gene1', 'gene2', 'gene2'])
    _expression_matrix_df_conflict = pd.DataFrame({'condition1': [1.1, 3.3], 'condition2': [1.1, 4.4]}, index=['gene1', 'gene1'])
    
    @pytest.fixture
    def expression_matrix_df(self):
        '''
        Simple valid matrix
        '''
        return self._expression_matrix_df.copy()
    
    @pytest.fixture
    def expression_matrix_df_duplicate_row(self):
        '''
        Valid matrix with a duplicate row
        '''
        return self._expression_matrix_df_duplicate_row.copy()
    
    @pytest.fixture
    def expression_matrix_df_conflict(self):
        '''
        Expression matrix with conflicting rows
        '''
        return self._expression_matrix_df_conflict.copy()
    
    def test_expression_matrix_invalid_type(self, session, expression_matrix_df):
        '''
        When matrix one of columns not float type, raise ValueError
        '''
        expression_matrix = expression_matrix_df
        expression_matrix['condition1'] = expression_matrix['condition1'].astype(int)
        with pytest.raises(ValueError) as ex:
            interpret.expression_matrix(session, expression_matrix)
        assert (dedent('''\
            Expression matrix values must be of type {}, column types of given matrix:
            condition1      int64
            condition2    float64'''
            ).format(np.dtype(float))
            in str(ex.value)
        )
            
    params = (
        (_expression_matrix_df, _expression_matrix_df),
        (_expression_matrix_df_duplicate_row, _expression_matrix_df_duplicate_row.iloc[0:2])
    )
    @pytest.mark.parametrize('original, expected', params)
    def test_happy_days(self, session, original, expected):
        '''
        When valid input, add all rows
        '''
        passed_in = original.copy()
        actual = interpret.expression_matrix(session, passed_in)
        df_.assert_equals(original, passed_in) # input unchanged
        actual.index = actual.index.to_series().apply(lambda x: x.name)
        df_.assert_equals(actual, expected)
        
    def test_conflict(self, session, expression_matrix_df_conflict):
        '''
        When a gene has multiple rows with different expression values, raise ValueError
        '''
        with pytest.raises(ValueError):
            interpret.expression_matrix(session, expression_matrix_df_conflict)
            
    def test_empty(self, session, expression_matrix_df):
        '''
        When adding an empty matrix, raise ValueError
        '''
        with pytest.raises(ValueError) as ex:
            interpret.expression_matrix(session, expression_matrix_df.loc[[]])
        assert 'Expression matrix must not be empty' in str(ex.value)
        